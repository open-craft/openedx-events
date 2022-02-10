"""
Tests for AvroAttrsBridge.
"""
from datetime import datetime
from unittest import TestCase

import attr
from opaque_keys.edx.keys import CourseKey

from openedx_events.avro_attrs_bridge import AvroAttrsBridge
from openedx_events.learning.data import (
    CourseData,
    CourseEnrollmentData,
    UserData,
    UserPersonalData,
)

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
            data={
                "enrollment": CourseEnrollmentData,
            },
        )
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

    def test_schema_evolution_producer_adds_value(self):
        """
        Make sure old consumer can handle serialized objects with old schema.
        """
        # Create object from old specification
        old_consumer_bridge = AvroAttrsBridge(
            self.SIGNAL_TO_SEND,
        )

        def inner_scope(self):
            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={"enrollment": CourseEnrollmentData, "new_key": str},
            )

            # Create new bridge(new consumer)
            new_producer_bridge = AvroAttrsBridge(
                SIGNAL_TO_SEND,
            )
            serialized_course_enrollment_data = new_producer_bridge.serialize_signal(
                enrollment=self.course_enrollment_data, new_key="new_value"
            )
            # Use new bridge to ready data serialized with old schema
            course_enrollment_data_from_wire = old_consumer_bridge.deserialize_signal(
                serialized_course_enrollment_data, new_producer_bridge.schema_dict
            )["enrollment"]

            assert course_enrollment_data_from_wire == self.course_enrollment_data

        inner_scope(self)

    def test_throw_exception_on_unextended_custom_type(self):
        """
        This test is in this class cause I need acces to self.assertRaises that comes from TestCase.
        I did not think it was worth it to create a whole new class for this test
        """
        with self.assertRaises(TypeError):

            class UnExtendedClass:
                pass

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={
                    "unextended_class": UnExtendedClass,
                },
            )
            # This should raise TypeError cause no extensions to serialize/deserialize UnExtededClass are being passed to bridge
            AvroAttrsBridge(SIGNAL_TO_SEND)

    def test_throw_exception_for_wrong_input(self):
        """
        Tests that a signal with complex attrs objects can be converted to dict and back
        """

        @attr.s(auto_attribs=True)
        class TestAttrData:
            sub_name: str
            course_id: str

        SIGNAL_TO_SEND = OpenEdxPublicSignal(
            event_type="org.openedx.test.test.test.v0",
            data={
                "string_type": str,
                "int_type": int,
                "float_type": float,
                "datetime_type": datetime,
                "attrs_type": TestAttrData,
            },
        )
        bridge = AvroAttrsBridge(SIGNAL_TO_SEND)
        # A test record that we can try to serialize to avro.
        test_data = {
            "string_type": "string_val",
            "int_type": 2,
            "float_type": 2.2,
            "datetime_type": datetime(1, 2, 3),
            "attrs_type": TestAttrData("sub_name", "course_id"),
        }
        _ = bridge.serialize_signal(**test_data)

        with self.assertRaises(TypeError):
            _ = bridge.serialize_signal(**test_data, extra_input="Why")

    def test_throw_exception_to_list_or_dict_types(self):
        """
        This test is in this class cause I need acces to self.assertRaises that comes from TestCase.
        I did not think it was worth it to create a whole new class for this test
        """
        # TODO: this exception should be more specific
        with self.assertRaises(Exception):

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={
                    "list_input": list,
                },
            )
            # This should raise TypeError cause no extensions to serialize/deserialize UnExtededClass are being passed to bridge
            AvroAttrsBridge(SIGNAL_TO_SEND)

        # TODO: this exception should be more specific
        with self.assertRaises(Exception):

            SIGNAL_TO_SEND = OpenEdxPublicSignal(
                event_type="org.openedx.test.test.test.v0",
                data={
                    "list_input": dict,
                },
            )
            # This should raise TypeError cause no extensions to serialize/deserialize UnExtededClass are being passed to bridge
            AvroAttrsBridge(SIGNAL_TO_SEND)


def test_object_evolution_producer_adds_value():
    @attr.s(auto_attribs=True)
    class TestData:
        sub_name: str
        course_id: str
        removed_key: str

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={
            "test_data": TestData,
        },
    )
    old_consumer_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={
            "test_data": TestData,
            "test_data2": TestData,
        },
    )
    new_producer_bridge = AvroAttrsBridge(SIGNAL_TO_SEND)

    test_data = TestData("sub_name", "course_id", "removed_value")
    serialized_record = new_producer_bridge.serialize_signal(
        test_data=test_data, test_data2=test_data
    )

    deserialized_obj = old_consumer_bridge.deserialize_signal(
        serialized_record, new_producer_bridge.schema_dict
    )
    assert deserialized_obj == {
        "test_data": TestData(
            sub_name="sub_name", course_id="course_id", removed_key="removed_value"
        )
    }


def test_nexted_attrs_object_serialization():
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
        data={
            "test_data": TestData,
        },
    )
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


def test_attrs_converstion_to_dict():
    """
    Tests that a signal with complex attrs objects can be converted to dict and back
    """

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
        data={
            "test_data": TestData,
        },
    )
    bridge = AvroAttrsBridge(SIGNAL_TO_SEND)
    # A test record that we can try to serialize to avro.
    test_data = TestData(
        "foo",
        "bar.course",
        1,
        SubTestData("a.sub.name", "a.nother.course"),
        SubTestData2("b.uber.sub.name", "b.uber.another.course"),
    )

    data_dict = bridge.to_dict({"test_data": test_data})
    assert data_dict == {
        "data": {
            "test_data": {
                "course_id": "bar.course",
                "name": "foo",
                "sub_test": {"course_id": "a.nother.course", "sub_name": "a.sub.name"},
                "uber_sub_test": {
                    "course_id": "b.uber.another.course",
                    "sub_name": "b.uber.sub.name",
                },
                "user_id": 1,
            }
        }
    }
    object_over_wire = bridge.dict_to_signal(data_dict["data"])
    assert object_over_wire == {"test_data": test_data}


def test_signal_converstion_to_dict():
    """
    Tests that a signal with complex attrs objects can be converted to dict and back
    """

    @attr.s(auto_attribs=True)
    class TestAttrData:
        sub_name: str
        course_id: str

    SIGNAL_TO_SEND = OpenEdxPublicSignal(
        event_type="org.openedx.test.test.test.v0",
        data={
            "string_type": str,
            "int_type": int,
            "float_type": float,
            "datetime_type": datetime,
            "attrs_type": TestAttrData,
        },
    )
    bridge = AvroAttrsBridge(SIGNAL_TO_SEND)
    # A test record that we can try to serialize to avro.
    test_data = {
        "string_type": "string_val",
        "int_type": 2,
        "float_type": 2.2,
        "datetime_type": datetime(1, 2, 3),
        "attrs_type": TestAttrData("sub_name", "course_id"),
    }
    data_dict = bridge.to_dict(test_data)
    assert data_dict == {
        "data": {
            "attrs_type": {"course_id": "course_id", "sub_name": "sub_name"},
            "datetime_type": "0001-02-03T00:00:00",
            "float_type": 2.2,
            "int_type": 2,
            "string_type": "string_val",
        }
    }
    object_over_wire = bridge.dict_to_signal(data_dict["data"])
    assert object_over_wire == test_data
