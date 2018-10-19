from threading import Lock
from p2ee.orm.models.base.fields import BaseField


class DocumentMetaClass(type):
    def __init__(cls, name, bases, dictionary):
        # Read from class variables to see if schema flexible is defined
        schema_flexible = dictionary.get('_schema_flexible', True)
        # Load the class
        super(DocumentMetaClass, cls).__init__(cls, bases, dictionary)
        # Set schema on class from class variables - Only BaseField allowed
        # cls._schema = {k: v for k, v in dictionary.items() if isinstance(v, BaseField)}
        cls._schema = {}
        for k, v in dictionary.items():
            if isinstance(v, BaseField):
                v.field_name = k
                cls._schema[k] = v
        # All class schemas are default flexible unless explicitly disabled
        cls._schema_flexible = schema_flexible
        cls._schema_lock = Lock()
        if len(cls.__mro__) > 1:
            # Merge values from super class
            try:
                cls._schema.update({k: v for k, v in cls.__mro__[1]._schema.items() if k not in cls._schema})
            except AttributeError:
                pass
            try:
                cls._schema_flexible = cls.__mro__[1]._schema_flexible and cls._schema_flexible
            except AttributeError:
                pass
