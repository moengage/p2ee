import json
import os
import requests
import sys

from p2ee.utils.environment import Environment


class PackageUtils(object):
    INSTANCE_META = None
    DEFAULT_INSTANCE_META = {
        "devpayProductCodes": None,
        "availabilityZone": "us-east-1c",
        "instanceId": "i-dummy",
        "region": "us-east-1",
        "marketplaceProductCodes": None,
        "pendingTime": "2017-11-17T11:41:09Z",
        "privateIp": "127.0.0.1",
        "version": "2017-09-30",
        "architecture": "x86_64",
        "billingProducts": None,
        "kernelId": None,
        "ramdiskId": None,
        "imageId": "ami-dummy",
        "instanceType": "localhost",
        "accountId": "dummy_account_id"
    }

    @classmethod
    def get_execution_env(cls):
        if not cls.EXECUTION_ENV:
            try:
                env = open(os.path.join(cls.get_config_local_base_folder(), 'moe_env')).read().strip() or 'prod'
            except IOError:
                env = os.environ.get('P2EE_DEPLOYMENT_ENV') or 'prod'
            cls.EXECUTION_ENV = Environment.from_str(env)
        return cls.EXECUTION_ENV

    @classmethod
    def get_config_local_base_folder(cls):
        return sys.prefix if cls.is_virtual_env() else '/etc'

    @classmethod
    def is_lambda_env(cls):
        valid_lambda_envs = ["AWS_Lambda_python2.7", "AWS_Lambda_python3.6"]
        return os.environ.get("AWS_EXECUTION_ENV") in valid_lambda_envs

    @classmethod
    def is_virtual_env(cls):
        return hasattr(sys, 'real_prefix')

    @classmethod
    def get_instance_meta(cls):
        if not PackageUtils.INSTANCE_META:
            try:
                response = requests.get('http://instance-data/latest/dynamic/instance-identity/document')
                cls.INSTANCE_META = json.loads(response.text)
            except Exception:
                return cls.DEFAULT_INSTANCE_META
        return cls.INSTANCE_META
