class InvalidFieldException(Exception):
    def __init__(self, message='Invalid field', field=None, missing=True):
        self.field = field
        self.missing = missing

        if self.field is not None:
            message = "{field}: {message}".format(field=field, message=message)

        super(InvalidFieldException, self).__init__(message)

    def get_message(self):
        message = ''

        if self.field is not None:
            message = "{field}: ".format(field=self.field)

        if self.missing:
            return "{field}{message}".format(field=message,
                                             message='Missing field(s)').strip(':').strip(' ')
        return "{field}{message}".format(
            field=message,
            message='Field(s) is/are not allowed'
        ).strip(':').strip(' ')


class InvalidFieldValueException(Exception):
    def __init__(self, message='Invalid field value', field=None, value=None):
        error_message = "{field} - {message} : {value}" if field else "{message} : {value}"
        super(InvalidFieldValueException, self).__init__(error_message.format(field=field, message=message,
                                                                              value=repr(value)))

    def get_message(self):
        return self.message


class InvalidFieldDefinition(Exception):
    def __init__(self, message='Invalid Field Definition', field=None):
        self.field = field
        super(InvalidFieldDefinition, self).__init__(message + ': ' + str(field))

    def get_message(self):
        return self.message
