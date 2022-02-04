#!/usr/bin/env python3


from openedx_events.avro_attrs_bridge import AvroAttrsBridge
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED

from openedx_events.learning.data import UserData, UserPersonalData

from openedx_events.tooling import OpenEdxPublicSignal

# TODO: Test with list
TestSignal = OpenEdxPublicSignal(
    event_type="org.openedx.learning.student.registration.completed.v1",
    data={
        "user": UserData,
        "number": int,
        "string": str,
    }
)
def test_complex():
    # TODO: this isn't a test
    bridge = AvroAttrsBridge(signal_cls=SESSION_LOGIN_COMPLETED)

    user_personal_data = UserPersonalData(
        username="username", email="email", name="name"
    )
    user_data = UserData(id=1, is_active=True, pii=user_personal_data)
    serialized_data = bridge.serialize_signal(user=user_data)
    deserialized_data = bridge.deserialize_signal(serialized_data)

def test_simple():
    bridge = AvroAttrsBridge(signal_cls=TestSignal)
    user_personal_data = UserPersonalData(
        username="username", email="email", name="name"
    )
    user_data = UserData(id=1, is_active=True, pii=user_personal_data)
    serialized_data = bridge.serialize_signal(user=user_data, number=5, string="Wow")
    deserialized_data = bridge.deserialize_signal(serialized_data)
    breakpoint()
