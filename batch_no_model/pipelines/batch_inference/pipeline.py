"""Example workflow Batch inference pipeline script for abalone pipeline.


    Process-> Inference

Implements a get_pipeline(**kwargs) method.
"""

import os

import boto3
import sagemaker
import sagemaker.session
from sagemaker.workflow.callback_step import (
    CallbackStep,
)
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from pipelines.batch_inference.pipeline_utils import upload_file_to_s3

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# target_env = os.environ.get("TARGET_ENV")

# REGION_NAME = os.environ.get("AWS_DEFAULT_REGION")
# EMR_APP_ID = os.environ.get(f"{target_env}_EMR_APP_ID")
# LAMBDA_FUNCTION_NAME = os.environ.get(f"{target_env}_LAMBDA_FUNCTION_NAME")
# SQS_QUEUE_URL = os.environ.get(f"{target_env}_SQS_QUEUE_URL")
# EMR_JOB_ROLE = os.environ.get(f"{target_env}_EMR_JOB_ROLE")
# RENI_FACTORY_CODE_BUCKET = os.environ.get(f"{target_env}_RENI_FACTORY_CODE_BUCKET")
# RENI_FACTORY_CODE_BUCKET_KEY = "shared/emr-serverless-job/home-cluster-segmentation"

target_env = "DEV"
TARGET_ENV = "DEV"
REGION_NAME = "ca-central-1"
EMR_APP_ID = "00fnt8gjssk0ed3d"
LAMBDA_FUNCTION_NAME = "sprouts_social_ingestion_callback"
SQS_QUEUE_URL = 'https://sqs.ca-central-1.amazonaws.com/975050255926/sprouts_social_ingestion_sqs'
EMR_JOB_ROLE = (
    "arn:aws:iam::975050255926:role/service-role/AmazonEMR-ServiceRole-20241114T164731"
)
RENI_FACTORY_CODE_BUCKET = "reni-dev-emr-bucket"
RENI_FACTORY_CODE_BUCKET_KEY = "shared/emr-serverless-job/test_pipeline"
SPROUT_SOCIAL_API_URL = "api.sproutsocial.com/v1"


required_env_vars = {
    "TARGET_ENV": target_env,
    "AWS_DEFAULT_REGION": REGION_NAME,
    f"{target_env}_EMR_JOB_ROLE": EMR_JOB_ROLE,
    f"{target_env}_EMR_APP_ID": EMR_APP_ID,
    f"{target_env}_LAMBDA_FUNCTION_NAME": LAMBDA_FUNCTION_NAME,
    f"{target_env}_SQS_QUEUE_URL": SQS_QUEUE_URL,
    f"{target_env}_RENI_FACTORY_CODE_BUCKET": RENI_FACTORY_CODE_BUCKET,
    f"{target_env}_SPROUT_SOCIAL_API_URL": SPROUT_SOCIAL_API_URL,
}


for var, value in required_env_vars.items():
    if not value or value.strip() == "" or value.strip() == "None":
        raise ValueError(
            f"The environment variable {var} is required and cannot be empty."
        )


def get_sagemaker_client(region):
    """Gets the sagemaker client.

    Args:
        region: the aws region to start the session
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        `sagemaker.session.Session instance
    """
    boto_session = boto3.Session(region_name=region)
    return boto_session.client("sagemaker")


def get_session(region, default_bucket):
    """Gets the sagemaker session based on the region.

    Args:
        region: the aws region to start the session
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        `sagemaker.session.Session instance
    """

    boto_session = boto3.Session(region_name=region)

    sagemaker_client = boto_session.client("sagemaker")
    runtime_client = boto_session.client("sagemaker-runtime")
    return sagemaker.session.Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        sagemaker_runtime_client=runtime_client,
        default_bucket=default_bucket,
    )


def get_pipeline_session(region, default_bucket):
    """Gets the pipeline session based on the region.

    Args:
        region: the aws region to start the session
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        PipelineSession instance
    """

    boto_session = boto3.Session(region_name=region)
    sagemaker_client = boto_session.client("sagemaker")

    return PipelineSession(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        default_bucket=default_bucket,
    )


def get_pipeline_custom_tags(new_tags, region, sagemaker_project_arn=None):
    try:
        sm_client = get_sagemaker_client(region)
        response = sm_client.list_tags(ResourceArn=sagemaker_project_arn)
        project_tags = response["Tags"]
        for project_tag in project_tags:
            new_tags.append(project_tag)
    except Exception as err:
        print(f"Error getting project tags: {err}")
    return new_tags


def get_pipeline(
    region,
    stage,  # staging o prod
    environment,  # preprod or prod
    account_id,
    role=None,
    default_bucket=RENI_FACTORY_CODE_BUCKET,
    pipeline_name="TesPipelineCICD",
    base_job_prefix="TesPipeline",
    project_name="TesPipeline",
):
    """Gets a SageMaker ML Pipeline instance working on abalone data in Spark.

    Args:
        region: AWS region to create and run the pipeline.
        role: IAM role to create and run steps and pipeline.
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        an instance of a pipeline
    """
    print(f"Role: {role}")
    session = boto3.Session(region_name=region)  # Example: 'ap-southeast-3'

    # Initialize the Lambda client
    lambda_client = session.client("lambda")

    # Initialize the SQS client
    sqs_client = session.client("sqs")

    sagemaker_session = get_session(region, default_bucket)
    if role is None:
        role = sagemaker.session.get_execution_role(sagemaker_session)

    # parameters for pipeline execution
    pipeline_session = get_pipeline_session(region, default_bucket)
    instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)
    instance_type = ParameterString(
        name="ProcessingInstanceType", default_value="ml.t3.medium"
    )
    yesterday = ParameterString(name="Yesterday", default_value="None")
    sprout_social_secret_manager_id = ParameterString(
        name="SproutSocialSecreteManagerId", default_value="reni-dev-sproute-social-secret"
    )
    red_shift_secret_manager_id = ParameterString(
        name="RedShiftSecreteManagerId",
        default_value="reni-dev-secret-redshift-cluster-1",
    )
    output_bucket = ParameterString(
        name="OutputBucket",
        default_value="reni-dev-dwh",
    )

    # Add event source mapping
    try:
        lambda_client.create_event_source_mapping(
            EventSourceArn=sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"],
            FunctionName=LAMBDA_FUNCTION_NAME,
            Enabled=True,
            BatchSize=1,
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass

    # Upload all pipeline steps scripts to s3
    script_folder_path = os.path.join(BASE_DIR, "scripts")
    script_upload_path = (
        f"s3://{RENI_FACTORY_CODE_BUCKET}/{RENI_FACTORY_CODE_BUCKET_KEY}"
    )

    # List all .py files in the script folder
    py_files = [f for f in os.listdir(script_folder_path) if f.endswith(".py")]

    # Upload each .py file to S3
    for script_file_name in py_files:
        script_file_path = os.path.join(script_folder_path, script_file_name)
        upload_file_to_s3(
            local_path=script_file_path,
            s3_uri=f"{script_upload_path}/{script_file_name}",
        )
        print(
            f"Uploaded script: {script_file_name} to path: {script_upload_path}/{script_file_name}"
        )

    # Step-1 ProfileIngestionJob
    test_name = "TestCI_CDJob"
    test_filename = "test_script.py.py"

    test_step = CallbackStep(
        name=test_name,
        sqs_queue_url=SQS_QUEUE_URL,
        inputs={
            "region_name": REGION_NAME,
            "job_name": f"{base_job_prefix}_{test_name}",
            "application_id": EMR_APP_ID,
            "script_path": f"{script_upload_path}/{test_filename}",
            "execution_role_arn": EMR_JOB_ROLE,
            "emr_env_bucket": RENI_FACTORY_CODE_BUCKET,
            "sprout_social_api_url": SPROUT_SOCIAL_API_URL,
            "yesterday": yesterday,
            "sprout_social_secret_manager_id": sprout_social_secret_manager_id,
            "red_shift_secret_manager_id": red_shift_secret_manager_id,
            "output_bucket": output_bucket,
        },
        outputs=[],
    )

    # pipeline instance

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            instance_count,
            instance_type,
            yesterday,
            sprout_social_secret_manager_id,
            red_shift_secret_manager_id,
            output_bucket,
        ],
        steps=[test_step],
        sagemaker_session=pipeline_session,
    )

    return pipeline
