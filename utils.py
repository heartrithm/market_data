import json
import sys

import boto3


IS_PYTEST = "pytest" in sys.modules


def get_aws_secret(secret_name):
    region_name = "us-west-2"

    if IS_PYTEST:
        return {"username": "test_user", "password": "test_pass"}

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])
