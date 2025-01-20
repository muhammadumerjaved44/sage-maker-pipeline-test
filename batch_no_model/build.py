import argparse
import json
import logging
import os
import sys
import datetime

import boto3
from botocore.exceptions import ClientError

from utils import (
    upload_file_to_s3,
    get_cfn_style_config
)

from pipelines import run_pipeline

LOG_FORMAT = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"

logger = logging.getLogger(__name__)

dev_acc_boto3_session = boto3.Session(profile_name="dev", region_name=os.environ.get('AWS_REGION'))
target_acc_boto3_session = boto3.Session(profile_name="target", region_name=os.environ.get('AWS_REGION'))
dev_sm_client = dev_acc_boto3_session.client("sagemaker")
target_sm_client = target_acc_boto3_session.client("sagemaker")
dev_s3_client = dev_acc_boto3_session.client("s3")
target_s3_client = target_acc_boto3_session.client("s3")
target_ecr_client = target_acc_boto3_session.client("ecr")
# ssm = target_acc_boto3_session.client("ssm")
target_env = os.environ.get('TARGET_ENV')

input_data_bucket = os.environ.get(f'{target_env}_INPUT_DATA_BUCKET')
output_data_bucket = os.environ.get(f'{target_env}_OUTPUT_DATA_BUCKET')

def extend_config(script_args, static_config):
    """Extend the stage configuration with additional parameters. SageMaker Project details
    and Model details. This function should be extended as required to pass on other
    dynamic configurations.
    """
    new_params = {
        "SageMakerProjectName": script_args.sagemaker_project_name,
        "SageMakerProjectId": script_args.sagemaker_project_id,
        "ProjectBucket": target_bucket,
        "Environment": script_args.environment,
        "SubnetIds": subnets,
        "SGIds": security_group_ids,
        "BatchPipeline": batch_pipeline,
        "PipelineDefinitionS3Key": s3_key_pipeline,
        "SageMakerRoleArn": os.environ.get(f'{target_env}_SAGEMAKER_ROLE_ARN')
    }

    if script_args.code == "tf":
        parameters_dict = {**static_config, **new_params}
        return parameters_dict

    new_tags = {}
    get_pipeline_custom_tags(script_args, dev_sm_client, new_tags)

    # replace the development stage and environment tags
    new_tags["development-stage"] = target_env.lower()
    new_tags["environment"] = "non-prod" if target_env.lower() == "staging"  else "prod"
    
    parameters_dict = {
        "Parameters": {**static_config["Parameters"], **new_params},
        "Tags": {**static_config.get("Tags", {}), **new_tags},
    }

    # return get_cfn_style_config(parameters_dict)
    return parameters_dict

def get_pipeline_custom_tags(args, sm_client, new_tags):
    try:
        response = sm_client.list_tags(
                ResourceArn=args.sagemaker_project_arn)
        project_tags = response["Tags"]
        for project_tag in project_tags:
            new_tags[project_tag["Key"]] = project_tag["Value"]
    except:
        logger.error("Error getting project tags")
    return new_tags

def create_cfn_params_tags_file(config, export_params_file, export_tags_file):
    # Write Params and tags in separate file for Cfn cli command
    parameters, tags = get_cfn_style_config(config)
    with open(export_params_file, "w") as f:
        logger.debug("parameters config: {}".format(json.dumps(parameters, indent=4)))
        json.dump(parameters, f, indent=4)
    with open(export_tags_file, "w") as f:
        logger.debug("tags config: {}".format(json.dumps(tags, indent=4)))
        json.dump(tags, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level", type=str, default=os.environ.get("LOGLEVEL", "INFO").upper()
    )
    parser.add_argument("--sagemaker-project-name", type=str, required=True)
    parser.add_argument("--sagemaker-project-id", type=str, required=True)
    parser.add_argument("--sagemaker-project-arn", type=str, required=True)
    parser.add_argument("--stage", type=str, required=True)
    parser.add_argument("--environment", type=str, required=True)
    parser.add_argument("--region", type=str, required=True)
    parser.add_argument("--training-id", type=str, required=True)
    parser.add_argument("--target-id", type=str, required=True)
    parser.add_argument("--import-config", type=str, required=True)
    parser.add_argument("--export-config", type=str, required=True)
    parser.add_argument("--export-tags", type=str, required=True)
    parser.add_argument("--pipeline-file", type=str, default="pipelinedefinition.json")
    parser.add_argument("--code", type=str, required=True)  # tf or cfn

    args, _ = parser.parse_known_args()

    # Configure logging to output the line number and message
    logging.basicConfig(format=LOG_FORMAT, level=args.log_level)

    # get target bucket, subnets and sg group from ssm
    target_bucket = os.environ.get('ARTIFACT_BUCKET')
    sagemaker_role_arn = os.environ.get(f'{target_env}_SAGEMAKER_ROLE_ARN')
    security_group_ids = os.environ.get(f'{target_env}_SG_IDS')
    subnets = os.environ.get(f'{target_env}_SUBNET_IDS')

    batch_pipeline = f"{args.sagemaker_project_name}-{args.sagemaker_project_id}-batch-no-model"

    # Build the pipeline
    pipeline_definition = run_pipeline.main(
        "pipelines.batch_inference.pipeline",
        sagemaker_role_arn,
        json.dumps(
            [
                {"Key": "sagemaker:project-name", "Value": args.sagemaker_project_name},
                {"Key": "sagemaker:project-id", "Value": args.sagemaker_project_id},
            ]
        ),
        json.dumps(
            {
                "region": args.region,
                "stage": args.stage,
                "environment": args.environment,
                "account_id": args.target_id,
                "role": sagemaker_role_arn,
                "default_bucket": target_bucket,
                "pipeline_name": batch_pipeline,
                "base_job_prefix": f"{args.sagemaker_project_name}-{args.sagemaker_project_id}",
                "project_name": args.sagemaker_project_name,
            }
        ),
    )
    logger.info(json.dumps(pipeline_definition, indent=4))

    # Upload pipeline definition on a json file and to s3
    pipeline_dct = json.loads(pipeline_definition)
    with open(f"{args.stage}-{args.pipeline_file}", "w") as f:
        f.write(json.dumps(pipeline_dct))
    timenow = datetime.datetime.now().strftime("%Y%m%d%H%M")
    s3_key_pipeline = f"{args.sagemaker_project_name}-{args.sagemaker_project_id}/pipeline/{args.stage}-v{timenow}-{args.pipeline_file}"
    upload_path = f"s3://{target_bucket}/{s3_key_pipeline}"

    upload_file_to_s3(
        f"{args.stage}-{args.pipeline_file}", upload_path, target_s3_client
    )
    logger.info("Uploaded: %s to %s", f"{args.stage}-{args.pipeline_file}", upload_path)

    # Extend the config file
    with open(args.import_config, "r", encoding="utf-8") as f:
        config = json.load(f)

    extended_config = extend_config(
        args,
        config,
    )

    create_cfn_params_tags_file(extended_config, args.export_config, args.export_tags)
