"""
Tests for AvroAttrsBridge.
"""
from datetime import datetime
from unittest import TestCase

import attr
from opaque_keys.edx.keys import CourseKey

from openedx_events.avro_attrs_bridge import AvroAttrsBridge
from openedx_events.avro_attrs_bridge_extensions import CourseKeyAvroAttrsBridgeExtension
from openedx_events.learning.data import CourseData, CourseEnrollmentData, UserData, UserPersonalData

from openedx_events.tooling import OpenEdxPublicSignal

class TestNoneBaseTypesInBridge(TestCase):
    """
    Tests to make sure AttrsAvroBridge handles custom types correctly.
    """

    def setUp(self):
        super().setUp()
        user_personal_data = UserPersonalData(
            username="username", email="email", name="name"
        )
        user_data = UserData(id=1, is_active=True, pii=user_personal_data)
        # define Coursedata, which needs Coursekey, which needs opaque key
        course_id = "course-v1:edX+DemoX.1+2014"
        course_key = CourseKey.from_string(course_id)
        course_data = CourseData(
            course_key=course_key,
            display_name="display_name",
            start=datetime.now(),
            end=datetime.now(),
        )
        self.SIGNAL_TO_SEND = OpenEdxPublicSignal(
            event_type="org.openedx.test.test.test.v0",
            data={"enrollment": CourseEnrollmentData,})
        self.course_enrollment_data = CourseEnrollmentData(
            user=user_data,
            course=course_data,
            mode="mode",
            is_active=False,
            creation_date=datetime.now(),
            created_by=user_data,
        )

    def test_non_base_types_in_bridge(self):
        """
        Test to makes ure AvroattrsBridge works correctly with non-attr classes.

        Specifically, testing to make sure the extension classes work as intended.
        """
        bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )
        serialized_course_enrollment_data = bridge.serialize_signal(
            enrollment=self.course_enrollment_data
        )
        object_from_wire = bridge.deserialize_signal(serialized_course_enrollment_data)
        assert {"enrollment": self.course_enrollment_data} == object_from_wire

    def test_schema_evolution_add_value(self):
        """
        Make sure new consumer can handle messasges created with old schema
        """
        # Create object from old specification
        old_bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )
        serialized_course_enrollment_data = old_bridge.serialize_signal(
            enrollment=self.course_enrollment_data
        )

        def inner_scope(self):
            # Evolve schema by adding is_active_2
            @attr.s(frozen=True)
            class CourseEnrollmentData:  # pylint: disable=redefined-outer-name
                """
                Temp class create to test schema evolution.
                """

                user = attr.ib(type=UserData)
                course = attr.ib(type=CourseData)
                mode = attr.ib(type=str)
                is_active = attr.ib(type=bool)
                creation_date = attr.ib(type=datetime)
                is_active_2 = attr.ib(type=bool, default=False)
                created_by = attr.ib(type=UserData, default=None)

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"enrollment": CourseEnrollmentData,})

            # Create new bridge(new consumer)
            new_bridge = AvroAttrsBridge(
                SIGNAL_TO_SEND,
            )
            # Use new bridge to ready data serialized with old schema
            object_from_wire = new_bridge.deserialize_signal(
                    serialized_course_enrollment_data, old_bridge.schema_dict
                )["enrollment"]
            # Since its easier to compare dicts over complex classes,
            # turning objects into dicts for comparison
            object_from_wire_as_dict = attr.asdict(object_from_wire)
            original_object_as_dict = attr.asdict(self.course_enrollment_data)
            # Adding new info to old object to make sure everything else but the new info(is_active 2) is the same
            original_object_as_dict["is_active_2"] = False
            assert object_from_wire_as_dict == original_object_as_dict
        inner_scope(self)

    def test_schema_evolution_remove_value(self):
        """
        Make sure old consumer can handle serialized objects with old schema.
        """
        # Create object from old specification
        old_bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )

        serialized_course_enrollment_data = old_bridge.serialize_signal(
            enrollment=self.course_enrollment_data
        )

        def inner_scope(self):
            # Evolve schema by removing "is_active"
            @attr.s(frozen=True)
            class CourseEnrollmentData:  # pylint: disable=redefined-outer-name
                """
                Temp class create to test schema evolution.
                """

                user = attr.ib(type=UserData)
                course = attr.ib(type=CourseData)
                mode = attr.ib(type=str)
                creation_date = attr.ib(type=datetime)
                created_by = attr.ib(type=UserData, default=None)

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"enrollment": CourseEnrollmentData,})

            # Create new bridge(new consumer)
            new_bridge = AvroAttrsBridge(
                SIGNAL_TO_SEND,
            )
            # Use new bridge to ready data serialized with old schema
            object_from_wire = new_bridge.deserialize_signal(
                    serialized_course_enrollment_data, old_bridge.schema_dict
                )["enrollment"]
            # Since its easier to compare dicts over complex classes,
            # turning objects into dicts for comparison
            object_from_wire_as_dict = attr.asdict(object_from_wire)

            original_object_as_dict = attr.asdict(self.course_enrollment_data)
            del original_object_as_dict["is_active"]
            assert object_from_wire_as_dict == original_object_as_dict

        inner_scope(self)

    def test_schema_evolution_add_complex_value(self):
        """
        Make sure new consumer can handle messasges created with old schema
        """
        # Create object from old specification
        old_bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )

        serialized_course_enrollment_data = old_bridge.serialize_signal(
            enrollment=self.course_enrollment_data
        )

        def inner_scope(self):

            user_personal_data = UserPersonalData(
                username="username", email="email", name="name"
            )
            user_data = UserData(id=1, is_active=True, pii=user_personal_data)

            @attr.s(frozen=True)
            class CourseEnrollmentData:  # pylint: disable=redefined-outer-name
                """
                Temp class create to test schema evolution.
                """

                user = attr.ib(type=UserData)
                course = attr.ib(type=CourseData)
                mode = attr.ib(type=str)
                is_active = attr.ib(type=bool)
                creation_date = attr.ib(type=datetime)
                created_by = attr.ib(type=UserData, default=None)
                user2 = attr.ib(type=UserData, default=user_data)

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"enrollment": CourseEnrollmentData,})

            # Create new bridge(new consumer)
            new_bridge = AvroAttrsBridge(
                SIGNAL_TO_SEND,
            )
            # Use new bridge to ready data serialized with old schema
            object_from_wire = new_bridge.deserialize_signal(
                    serialized_course_enrollment_data, old_bridge.schema_dict
                )["enrollment"]
            # Since its easier to compare dicts over complex classes,
            # turning objects into dicts for comparison
            object_from_wire_as_dict = attr.asdict(object_from_wire)

            original_object_as_dict = attr.asdict(self.course_enrollment_data)
            original_object_as_dict["user2"] = attr.asdict(user_data)
            assert object_from_wire_as_dict == original_object_as_dict

        inner_scope(self)

    def test_schema_evolution_add_complex_extension_value(self):
        # Create object from old specification
        old_bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )

        serialized_course_enrollment_data = old_bridge.serialize_signal(
            enrollment=self.course_enrollment_data
        )

        def inner_scope(self):

            @attr.s(frozen=True)
            class CourseEnrollmentData:  # pylint: disable=redefined-outer-name
                """
                Temp class create to test schema evolution.
                """

                user = attr.ib(type=UserData)
                course = attr.ib(type=CourseData)
                mode = attr.ib(type=str)
                is_active = attr.ib(type=bool)
                creation_date = attr.ib(type=datetime)
                created_by = attr.ib(type=UserData, default=None)
                added_date = attr.ib(type=datetime, default=None)

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"enrollment": CourseEnrollmentData,})

            # Create new bridge(new consumer)
            new_bridge = AvroAttrsBridge(
                SIGNAL_TO_SEND,
            )
            # Use new bridge to ready data serialized with old schema
            object_from_wire = new_bridge.deserialize_signal(
                    serialized_course_enrollment_data, old_bridge.schema_dict
                )["enrollment"]

            # Since its easier to compare dicts over complex classes,
            # turning objects into dicts for comparison
            object_from_wire_as_dict = attr.asdict(object_from_wire)

            original_object_as_dict = attr.asdict(self.course_enrollment_data)
            original_object_as_dict["added_date"] = None
            assert object_from_wire_as_dict == original_object_as_dict
        inner_scope(self)

    def test_throw_exception_on_unextended_custom_type(self):
        """
        This test is in this class cause I need acces to self.assertRaises that comes from TestCase.
        I did not think it was worth it to create a whole new class for this test
        """
        with self.assertRaises(TypeError):
            class UnExtendedClass():
                pass

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"unextended_class": UnExtendedClass,})
            # This should raise TypeError cause no extensions to serialize/deserialize UnExtededClass are being passed to bridge
            AvroAttrsBridge(SIGNAL_TO_SEND)



def test_object_evolution_add_value():
    @attr.s(auto_attribs=True)
    class TestData:
        sub_name: str
        course_id: str

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={"test_data": TestData,})
    original_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)

    test_data = TestData("sub_name", "course_id")
    serialized_record = original_bridge.serialize_signal(test_data=test_data)

    @attr.s(auto_attribs=True)
    class TestData:  # pylint: disable=function-redefined
        sub_name: str
        course_id: str
        added_key: str = "default_value"

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={"test_data": TestData,})
    new_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)
    deserialized_obj = new_bridge.deserialize_signal(
        serialized_record, original_bridge.schema_dict
    )
    assert deserialized_obj == {"test_data": TestData(
        sub_name="sub_name", course_id="course_id", added_key="default_value"
    )}


def test_object_evolution_remove_value():
    @attr.s(auto_attribs=True)
    class TestData:
        sub_name: str
        course_id: str
        removed_key: str

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={"test_data": TestData,})
    original_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)

    test_data = TestData("sub_name", "course_id", "removed_value")
    serialized_record = original_bridge.serialize_signal(test_data=test_data)

    @attr.s(auto_attribs=True)
    class TestData:  # pylint: disable=function-redefined
        sub_name: str
        course_id: str


    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={"test_data": TestData,})
    new_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)
    deserialized_obj = new_bridge.deserialize_signal(
        serialized_record, original_bridge.schema_dict
    )
    assert deserialized_obj == {"test_data": TestData(sub_name="sub_name", course_id="course_id")}


def test_base_types():
    @attr.s(auto_attribs=True)
    class SubTestData:
        sub_name: str
        course_id: str

    @attr.s(auto_attribs=True)
    class SubTestData2:
        sub_name: str
        course_id: str

    @attr.s(auto_attribs=True)
    class TestData:  # pylint: disable=missing-class-docstring
        name: str
        course_id: str
        user_id: int
        sub_test: SubTestData
        uber_sub_test: SubTestData2

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={"test_data": TestData,})
    bridge = AvroAttrsBridge(SIGNAL_TO_SEND)

    # A test record that we can try to serialize to avro.
    test_data = TestData(
        "foo",
        "bar.course",
        1,
        SubTestData("a.sub.name", "a.nother.course"),
        SubTestData2("b.uber.sub.name", "b.uber.another.course"),
    )
    serialized_record = bridge.serialize_signal(test_data=test_data)

    # Try to de-serialize back to an attrs class.
    object_from_wire = bridge.deserialize_signal(serialized_record)
    assert {"test_data": test_data} == object_from_wire
