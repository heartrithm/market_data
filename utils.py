import json

import boto3


def get_aws_secret(secret_name):
    region_name = "us-west-2"

    if settings.TESTS_RUNNING:
        return {"username": "test_user", "password": "test_pass"}

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])
