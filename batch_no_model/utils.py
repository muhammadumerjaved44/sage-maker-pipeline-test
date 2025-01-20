import logging
import os
import time
from typing import Union

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_bucket_and_key(s3_uri: str) -> str:
    """Function to return bucket name and key for a given s3 uri.
    Args: s3_uri: s3 uri such as s3://bucket/file.txt or s3://bucket/prefix
    Returns: bucket_name, key
    """
    uri_parts = s3_uri.replace("s3://", "").split("/")
    return uri_parts[0], "/".join(uri_parts[1:])


def upload_file_to_s3(local_path: str, s3_uri: str, s3_client=None) -> None:
    """Function to upload file to s3
    Args:
        local_path: local path of file.
        s3_uri: s3 location of file such as s3://bucket/file.txt
        s3_client: boto3.client("s3"). If not provided, one is created with default credentials.
    Returns:
    """
    if not s3_client:
        s3_client = boto3.client("s3", region_name=os.environ.get('AWS_REGION'))
    bucket, key = get_s3_bucket_and_key(s3_uri)
    s3_client.upload_file(local_path, bucket, key)


def get_cfn_style_config(stage_parameters: dict) -> list:
    """Change formatting of config for CloudFormation"""
    parameters = []
    for key, value in stage_parameters["Parameters"].items():
        param = f"{key}={value}"
        parameters.append(param)
    tags = []
    for key, value in stage_parameters["Tags"].items():
        tag = f"{key}={value}"
        tags.append(tag)
    return parameters, tags
