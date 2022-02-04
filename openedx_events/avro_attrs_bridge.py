"""
Code to convert attr classes to avro specification.
"""
import io
import json
import uuid
from datetime import datetime
from typing import Any, Dict

import attr
import fastavro

from openedx_events.avro_attrs_bridge_extensions import DatetimeAvroAttrsBridgeExtension, CourseKeyAvroAttrsBridgeExtension

from openedx_events.avro_types import PYTHON_TYPE_TO_AVRO_MAPPING


class AvroAttrsBridge:
    """
    Use to convert between Avro and Attrs data specifications.

    Intended usecase: To abstract serilalization and deserialization of openedx-events to send over pulsar or kafka
    """

    # default extensions, can be overwriteen by passing in extensions during obj initialization
    default_extensions = {
        DatetimeAvroAttrsBridgeExtension.cls: DatetimeAvroAttrsBridgeExtension(),
        CourseKeyAvroAttrsBridgeExtension.cls: CourseKeyAvroAttrsBridgeExtension(),
    }
    def __init__(self, signal_cls, extensions=None):
        """
        Init method for Avro Attrs Bridge.

        Arguments:
            attrs_cls: Attr Class Object (not instance)
            extensions: dict mapping Class Object to its AvroAttrsBridgeExtention subclass instance
        """

        self.extensions = {}
        self.extensions.update(self.default_extensions)
        if isinstance(extensions, dict):
            self.extensions.update(extensions)

        self._signal_cls = signal_cls
        # Used by record_field_for_attrs_class function to keep track of which
        # records have already been defined in schema.
        # Reason: fastavro does no allow you to define record with same name twice
        self.schema_record_names = set()
        self.schema_dict = self._signal_to_avro_schema(signal_cls)
        # make sure the schema is parsable
        fastavro.parse_schema(self.schema_dict)

    def schema_str(self):
        """Json dumps schema dict into a string."""
        return json.dumps(self.schema_dict, sort_keys=True)

    def _signal_to_avro_schema(self, signal_cls):
        """
        Generate avro schema for attr_cls.

        Arguments:
            attrs_cls: Attr class object
        Returns:
            complex dict that defines avro schema for attrs_cls
        """

        base_schema = {
            "type": "record",
            "name": "OpenedxSignal",
            "fields": [
            ],
        }
        record_fields = self._record_fields_from_data_dict(signal_cls)
        base_schema["fields"].append(record_fields)
        return base_schema

    def _record_fields_from_data_dict(self, cls):
        field: Dict[str, Any] = {}
        field["name"] = "data"
        field["type"] = dict(name='OpenEdxPublicSignal', type="record", fields=[])
        for key_name, data_type in cls.init_data.items():
            field["type"]["fields"].append(self._get_object_fields(key_name, data_type))
        return field


    def _get_object_fields(self, object_name, _object_type,if_default=False, default=None):
        inner_field = {"name": object_name}
        if (extension := self.extensions.get(_object_type, None)):
            inner_field = {
                "name": object_name,
                "type": extension.record_fields(),
            }
        # Attribute is a simple type.
        elif _object_type in PYTHON_TYPE_TO_AVRO_MAPPING:
            if PYTHON_TYPE_TO_AVRO_MAPPING[_object_type] in ["record", "array"]:
                # TODO: figure out how to handle container types. This will require better specifications for types in openedx-events repositories.
                # This is a conversation that needs to be had with others, for now, these data types will not be supported by this bridge.
                raise Exception("item type for container object not specified")
            inner_field = {
                "name": object_name,
                "type": PYTHON_TYPE_TO_AVRO_MAPPING[_object_type],
            }

        # _object_type is another attrs class
        elif hasattr(_object_type, "__attrs_attrs__"):
            # Inner Attrs Class

            # fastavro does not allow you to redefine the same record type more than once,
            # so only define an attr record once
            if _object_type.__name__ in self.schema_record_names:
                inner_field = {
                    "name": object_name,
                    "type": _object_type.__name__,
                }
            else:
                self.schema_record_names.add(_object_type.__name__)
                inner_field = self._record_fields_for_attrs_class(
                    _object_type, object_name
                )
        # else _object_type is an costom type and
        # there needs to be AvroAttrsBridgeExtension for _object_type in self.extensions
        else:
            raise TypeError(
                f"Object type {_object_type} is not supported by AvroAttrsBridge. The object type needs to either be one of the types in PYTHON_TYPE_TO_AVRO_MAPPING, attrs decorated class, or one of the types defined in self.extensions"
            )
        # Assume _object_type is optional if it has a default value
        # The default value is always set to None to allow attr class to handle dealing with default values
        # in dict_to_attrs function in this class
        # TODO make sure defaults are handled properly
        if if_default and default is not attr.NOTHING:
            inner_field["type"] = ["null", inner_field["type"]]
            inner_field["default"] = None

        return inner_field

    def _record_fields_for_attrs_class(
        self, attrs_class, field_name: str) -> Dict[str, Any]:
        """
        Generate avro record for attrs_class.

        Will also recursively generate avro records for any sub attr classes and any custom types.

        Custom types are handled by AvroAttrsBridgeExtention subclass instance values defined in self.extensions dict
        """
        field: Dict[str, Any] = {}
        field["name"] = field_name
        field["type"] = dict(name=attrs_class.__name__, type="record", fields=[])

        for attribute in attrs_class.__attrs_attrs__:
            # TODO does get_object_fields need ot attribute.name
            field["type"]["fields"].append(self._get_object_fields(attribute.name, attribute.type, True, attribute.default))
        return field

    def to_dict(self, data_dict):
        """
        Convert data_dict into dictionary that matches avro schema (self.schema).

        Args:
            data_dict: dict with all the values specified in OpenEdxPublicSignal.init_data.
        """
        # TODO: is there better way to do this besides using json?
        # This first converts data_dict to json string and then back to dict.
        # The goal: Covert everything with type is self.extensions is coverted to serializable formats and
        return {"data": json.loads(json.dumps(data_dict,  sort_keys=True, default=self._extension_signal_serializer))}

    def serialize_signal(self, **kwargs) -> bytes:
        """
        Convert from attrs to a valid avro record.
        """
        # Try to serialize using the generated schema.
        out = io.BytesIO()
        data_dict = self.to_dict(kwargs)
        # TODO I'm using json to covert dict of complex object into a dic of not complex object. Is that a good idea?
        data_dict = {"data": json.loads(json.dumps(kwargs,  sort_keys=True, default=self._extension_signal_serializer))}
        fastavro.schemaless_writer(out, self.schema_dict, data_dict)
        out.seek(0)
        return out.read()

    def _extension_serializer(self, _, field, value):
        """
        Pass this callback into attrs.asdict function as "value_serializer" arg.
        Serializes values for which an extention exists in self.extensions dict.
        """
        extension = self.extensions.get(field.type, None)
        if extension is not None:
            return extension.serialize(value)
        return value

    def _extension_signal_serializer(self, value):
        """
        TODO: fix documentation
        This one is used for JSON serialization.
        Pass this callback into attrs.asdict function as "value_serializer" arg.

        Serializes values for which an extention exists in self.extensions dict.
        """
        if hasattr(value, "__attrs_attrs__"):
            return attr.asdict(value, value_serializer=self._extension_serializer)
        extension = self.extensions.get(type(value), None)
        if extension is not None:
            return extension.serialize(value)
        return value


    def deserialize_signal(self, data: bytes, writer_schema=None) -> object:
        """
        Deserialize data into self.signal class instance.
        TODO: fix documentation for signal

        Args:
            data: bytes that you want to deserialize
            writer_schema: pass the schema used to serialize data if it is differnt from current schema
        """
        data_file = io.BytesIO(data)
        if writer_schema is not None:
            record = fastavro.schemaless_reader(
                data_file, writer_schema, self.schema_dict
            )
        else:
            record = fastavro.schemaless_reader(data_file, self.schema_dict)
        return self.dict_to_signal(record["data"])

    def dict_to_signal(self, data):
        """
        TODO: document
        """
        for key, data_type in self._signal_cls.init_data.items():
            if key in data:
                if hasattr(data_type, "__attrs_attrs__"):
                    data[key] = self.dict_to_attrs(data[key], data_type)
                elif type(data[key]) == data_type:
                    pass
                elif data_type in self.extensions:
                    extension = self.extensions.get(data_type)
                    data[key] = extension.deserialize(data[key])
                else:
                    breakpoint()
        return data

    def dict_to_attrs(self, data: dict, attrs_cls):
        """
        Convert data into instantiated object of attrs_cls.
        """
        for attribute in attrs_cls.__attrs_attrs__:
            if attribute.name in data:
                sub_data = data[attribute.name]
                if sub_data is None:
                    # delete keys that have defaults in attr class and which have None as value
                    # this is to let attr class take care of creating default values
                    if attribute.default is not attr.NOTHING:
                        del data[attribute.name]
                else:
                    if hasattr(attribute.type, "__attrs_attrs__"):
                        if attribute.name in data:
                            data[attribute.name] = self.dict_to_attrs(
                                sub_data, attribute.type
                            )
                    elif attribute.type in self.extensions:
                        extension = self.extensions.get(attribute.type)
                        if attribute.name in data:
                            data[attribute.name] = extension.deserialize(sub_data)
                        else:
                            raise Exception(
                                f"Necessary key: {attribute.name} not found in data dict"
                            )
                    elif attribute.type not in PYTHON_TYPE_TO_AVRO_MAPPING:
                        raise TypeError(
                            f"Unable to deserialize {attribute.type} data, please add extension for custom data type"
                        )

        return attrs_cls(**data)


class AvroAttrsBridgeKafkaWrapper(AvroAttrsBridge):
    """
    TODO: modify class to handle signals instead of attrs
    Wrapper class to help AvroAttrsBridge to work with kafka.
    """

    def to_dict(self, obj, _kafka_context):  # pylint: disable=signature-differs
        """
        Pass this function as callable input to confluent_kafka::AvroSerializer.
        """
        return super().to_dict(obj)

    def from_dict(self, data, _kafka_context):
        """
        Pass this function as callable input to confluent_kafka::AvroDeSerializer.
        """
        return self.dict_to_attrs(data["data"], self._attrs_cls)
