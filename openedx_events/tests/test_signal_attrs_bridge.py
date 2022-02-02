#!/usr/bin/env python3


from openedx_events.avro_attrs_bridge import AvroAttrsBridge
from openedx_events.learning.signals import SESSION_LOGIN_COMPLETED

def test_simple():
    bridge = AvroAttrsBridge(signal_cls=SESSION_LOGIN_COMPLETED)
