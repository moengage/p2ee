from p2ee.orm.models.enums import StringEnum


class Environment(StringEnum):
    PROD = "prod"
    PREPROD = "pp"
    STAGING = "staging"
    DEVELOPMENT = "dev"
