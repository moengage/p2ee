"""Field types

Reference:
    - mongoengine.base.fields
    - mongoengine.fields
"""
import re
from abc import ABCMeta

import datetime
import six
from bson import ObjectId
from dateutil import parser
from enum import Enum

from p2ee.orm.models.exceptions import InvalidFieldValueException, InvalidFieldException, InvalidFieldDefinition

__all__ = ('BaseField', 'StringField', 'IntField', 'EmailField', 'ObjectIdField', 'ListField', 'DictField',
           'EmbeddedField', 'BooleanField', 'EnumField', 'DBNameField', 'DateTimeField', 'FloatField',
           'ObjectField', 'MultiEmbeddedListField')


class BaseField(object):
    __metaclass__ = ABCMeta
    """A base class for all types of fields.

    Default value: None
    """

    def __init__(self, default=None, choices=None, required=False, field_name=None, allowed_types=None):
        """
        :param default: (optional) The default value for this field if
            no value has been set (or if the value has been unset).
            It can be a callable.
        :param choices: (optional) The valid choices
        """
        self._default = default
        self._choices = choices
        self._required = required
        self._field_name = field_name
        self._allowed_types = allowed_types or ()

    @property
    def default(self):
        """Default value for the field"""
        value = self._default if not callable(self._default) else self._default()
        if self.required and value is None:
            raise InvalidFieldValueException(message='Field is required but value is None', field=self.field_name)
        return value

    @property
    def choices(self):
        """Default value for the field"""
        return self._choices

    @property
    def required(self):
        return self._required

    @property
    def field_name(self):
        return self._field_name

    @field_name.setter
    def field_name(self, name):
        """The default value for this field is the key name if set from the model using it"""
        if self._field_name is None:
            self._field_name = name

    def _instance_check(self, value):
        if not isinstance(value, self._allowed_types):
            self._raise_check_failure(value)
        return value

    def _raise_check_failure(self, value, message=None):
        if not message:
            failure_message_format = 'Value must be of type {expected_type}, passed type: {value_type}'
            failure_message = failure_message_format.format(expected_type=str(self._allowed_types),
                                                            value_type=str(type(value)))
        else:
            failure_message = message
        raise InvalidFieldValueException(failure_message,
                                         field=self.field_name, value=value)

    def validate(self, value):
        """Derived class should override this method and add extra validation logic."""
        value = self._instance_check(value)
        if self.required and value is None:
            raise InvalidFieldValueException('Value cannot be None',
                                             field=self.field_name, value=value)
        if self.choices is not None and value not in self.choices:
            raise InvalidFieldValueException('Value must be one of the permitted values: %r' % self.choices,
                                             field=self.field_name, value=value)

        return value

    @classmethod
    def _validate_validator(cls, validator):
        if validator and not isinstance(validator, BaseField):
            raise InvalidFieldDefinition("Element Validator should be an instance of BaseField: %r" % validator)
        return validator


class BoundedField(BaseField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self._min_value = min_value
        self._max_value = max_value
        super(BoundedField, self).__init__(**kwargs)

    @property
    def min_value(self):
        return self._min_value() if callable(self._min_value) else self._min_value

    @property
    def max_value(self):
        return self._max_value() if callable(self._max_value) else self._max_value

    def _min_check(self, value):
        if self.min_value is not None and value < self.min_value:
            self._raise_check_failure(value, 'Value is less than min value: %r' % self.min_value)

    def _max_check(self, value):
        if self.max_value is not None and value > self.max_value:
            self._raise_check_failure(value, 'Value is greater than max value: %r' % self.max_value)

    def validate(self, value):
        value = super(BoundedField, self).validate(value)
        self._min_check(value)
        self._max_check(value)
        return value


class StringField(BaseField):
    """A unicode string field.

    Default value:
    """

    def __init__(self, regex=None, min_length=None, max_length=None, allow_empty=True, **kwargs):
        self.min_length = min_length
        self.max_length = max_length
        self.allow_empty = allow_empty

        try:
            self.regex = re.compile(regex) if regex is not None else None
        except Exception:
            raise InvalidFieldValueException('Invalid regex pattern')

        super(StringField, self).__init__(allowed_types=six.string_types, **kwargs)

    def validate(self, value):
        value = super(StringField, self).validate(value)

        if self.max_length is not None and len(value) > self.max_length:
            self._raise_check_failure(value, 'String value too long, max length: %r' % self.max_length)

        if self.min_length is not None and len(value) < self.min_length:
            self._raise_check_failure(value, 'String value too short, min length: %r' % self.min_length)

        if self.regex is not None and self.regex.match(value) is None:
            self._raise_check_failure(value, 'String value did not match validation regex: %r' % self.regex)

        if not self.allow_empty and re.compile("^(?!\s*$).+").match(value) is None:
            self._raise_check_failure(value, 'String value empty not allowed')

        return value


class ObjectIdField(BaseField):
    def __init__(self, default=ObjectId, **kwargs):
        super(ObjectIdField, self).__init__(default=default, allowed_types=ObjectId, **kwargs)

    def _instance_check(self, value):
        val = value
        if not isinstance(value, self._allowed_types):
            if value and len(value) == 24 and ObjectId.is_valid(value):
                val = ObjectId(value)
            else:
                self._raise_check_failure(value)
        return val


class DBNameField(StringField):
    def __init__(self, **kwargs):
        super(DBNameField, self).__init__(max_length=100, min_length=1, **kwargs)


class EnumField(BaseField):
    def __init__(self, enum_class=Enum, **kwargs):
        self.enum_class = enum_class
        if not hasattr(self.enum_class, 'fromStr'):
            raise InvalidFieldException("Enum class %r must implement `fromStr` method" % self.enum_class)
        choices = kwargs.pop('choices', None) or list(self.enum_class)
        super(EnumField, self).__init__(choices=choices, allowed_types=self.enum_class, **kwargs)

    def _instance_check(self, value):
        if not isinstance(value, self.enum_class):
            value_enum = self.enum_class.fromStr(value)
            if not value_enum:
                self._raise_check_failure(
                    value_enum,
                    "Enum doesnt allow value: %r, allowed values: %r" % (value_enum, list(self.enum_class))
                )
        else:
            value_enum = value
        return super(EnumField, self)._instance_check(value_enum)


class IntField(BoundedField):
    """32-bit integer field.

    Default value: 0
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        super(IntField, self).__init__(min_value=min_value, max_value=max_value, allowed_types=(int, long,), **kwargs)

    def _instance_check(self, value):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        return super(IntField, self)._instance_check(value)


class EmailField(StringField):
    """A field that validates input as an email address.

    Default value: ''
    """
    EMAIL_REGEX = re.compile(
        # dot-atom
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
        # quoted-string
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"'
        # domain
        r')@(?:[A-Z0-9](?:[A-Z0-9-]{0,253}[A-Z0-9])?\.)+[A-Z]{2,6}$', re.IGNORECASE
    )

    def validate(self, value):
        value = super(EmailField, self).validate(value)
        if not EmailField.EMAIL_REGEX.match(value):
            self._raise_check_failure(value, 'Invalid email address')
        return value


class MultiEmbeddedListField(BaseField):
    """
        This class support multiple types of embedded field for ListField and validates them.
    """
    def __init__(self, element_validators=None, choices=None, max_items=None, **kwargs):
        self.element_validators = self._validate_validator(element_validators)
        self.max_items = max_items
        self.container_type = kwargs.pop('container_type', list)
        super(MultiEmbeddedListField, self).__init__(allowed_types=(list, tuple, set,), **kwargs)

    def add_value(self, container, value):
        if self.container_type is list:
            container.append(value)
        elif self.container_type is set:
            container.add(value)

    @classmethod
    def _validate_validator(cls, validator):
        if validator is not None and not isinstance(validator, list):
            raise InvalidFieldDefinition("Element Validators should be a list of validator")
        for v in validator:
            super(MultiEmbeddedListField, cls)._validate_validator(v)
        return validator

    def validate(self, value):
        """Make sure that a list of valid fields is being used."""
        value = self._instance_check(value)

        container = self.container_type()
        if self.element_validators:
            validators_name = []
            for val in value:
                validated_value = None
                for element_validator in self.element_validators:
                    try:
                        validated_value = element_validator.validate(val)
                        break
                    except InvalidFieldValueException:
                        if isinstance(element_validator, EmbeddedField):
                            validators_name.append(element_validator.document.__name__)
                        else:
                            validators_name.append(element_validator.__class__.__name__)
                        continue
                if validated_value:
                    self.add_value(container, validated_value)
                else:
                    message = ', '.join(validators_name)
                    raise InvalidFieldDefinition('Value must be one of the permitted types :%s' % message,
                                                 field=self.field_name)
        else:
            if not isinstance(value, self.container_type):
                container = self.container_type(value)
            else:
                container = value

        if self.max_items and len(container) > self.max_items:
            self._raise_check_failure(len(container), "Too many items in list, max allowed: %d" % self.max_items)

        return super(MultiEmbeddedListField, self).validate(container)


class ListField(MultiEmbeddedListField):
    """A list field that wraps a standard field.
    `items` are converted to validator type if validator is passed

    Default value: []
    """

    def __init__(self, element_validator=None, choices=None, max_items=None, **kwargs):
        element_validators = [element_validator] if element_validator else []
        super(ListField, self).__init__(element_validators=element_validators, choices=choices, max_items=max_items,
                                        **kwargs)


class DictField(BaseField):
    """A dictionary field that parses a standard Python dictionary.
    `keys` and `values` are converted to validator types if validators are passed

    Default value: {}
    """

    def __init__(self, key_validator=None, value_validator=None, choices=None, **kwargs):
        # Skip choices and dont pass it anywhere. Not allowed for DictField
        self.key_validator = self._validate_validator(key_validator)
        self.value_validator = self._validate_validator(value_validator)

        super(DictField, self).__init__(allowed_types=dict, **kwargs)

    def validate(self, value):
        """Make sure that a list of valid fields is being used."""
        value = self._instance_check(value)
        value_dict = {}

        if self.key_validator is not None or self.value_validator is not None:
            for key, val in value.iteritems():
                if self.key_validator is not None:
                    validated_key = self.key_validator.validate(key)
                else:
                    validated_key = key

                if self.value_validator is not None:
                    validated_value = self.value_validator.validate(val)
                else:
                    validated_value = val
                value_dict[validated_key] = validated_value
        else:
            value_dict = value

        return super(DictField, self).validate(value_dict)


class EmbeddedField(BaseField):
    """A SimpleDocument field that wraps a Simple document object."""

    def __init__(self, document, **kwargs):
        from moengage.models.base import SimpleDocument
        if not issubclass(document, SimpleDocument):
            raise InvalidFieldValueException('Invalid document', field=self.field_name)

        self.document = document
        super(EmbeddedField, self).__init__(allowed_types=self.document, **kwargs)

    def validate(self, value, field=None):
        doc = self.document(**value) if isinstance(value, dict) else value
        return super(EmbeddedField, self).validate(doc)


class BooleanField(BaseField):
    def __init__(self, **kwargs):
        kwargs.setdefault('default', False)
        super(BooleanField, self).__init__(allowed_types=bool, **kwargs)


class DateTimeField(BoundedField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        kwargs.setdefault('default', datetime.datetime.utcnow)
        self.ignoretz = kwargs.pop('ignoretz', True)
        super(DateTimeField, self).__init__(min_value, max_value, allowed_types=datetime.datetime, **kwargs)

    def _instance_check(self, value):
        if value is not None and not isinstance(value, datetime.datetime):
            try:
                value = parser.parse(value, ignoretz=self.ignoretz)
            except ValueError:
                self._raise_check_failure(value)
        return super(DateTimeField, self)._instance_check(value)


class FloatField(BoundedField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        super(FloatField, self).__init__(min_value=min_value, max_value=max_value, allowed_types=float, **kwargs)

    def _instance_check(self, value):
        if isinstance(value, (long, int)):
            value = float(value)
        return super(FloatField, self)._instance_check(value)


class ObjectField(BaseField):
    def __init__(self, **kwargs):
        super(ObjectField, self).__init__(allowed_types=object, **kwargs)
