import sys
from datetime import datetime

import boto3
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Access command-line arguments
arguments = sys.argv[1:]

# Extract command-line arguments
token = arguments[arguments.index("--token") + 1] if "--token" in arguments else None
region_name = (
    arguments[arguments.index("--region_name") + 1]
    if "--region_name" in arguments
    else None
)


# Initialize a session
session = boto3.Session(region_name=region_name)

# Initialize the sagemaker and s3 clients
s3_client = session.client("s3")
sagemaker_client = session.client("sagemaker")

OUTPUT_BUCKET_KEY = "staging-data/test-pipeline/test-data"

try:

    sprout_social_api_url = (
        arguments[arguments.index("--sprout_social_api_url") + 1]
        if "--sprout_social_api_url" in arguments
        else None
    )

    yesterday = (
        arguments[arguments.index("--yesterday") + 1]
        if "--yesterday" in arguments
        else None
    )

    sprout_social_secret_manager_id = (
        arguments[arguments.index("--sprout_social_secret_manager_id") + 1]
        if "--sprout_social_secret_manager_id" in arguments
        else None
    )

    red_shift_secret_manager_id = (
        arguments[arguments.index("--red_shift_secret_manager_id") + 1]
        if "--red_shift_secret_manager_id" in arguments
        else None
    )

    output_bucket = (
        arguments[arguments.index("--output_bucket") + 1]
        if "--output_bucket" in arguments
        else None
    )

    print(
        "Args:\n"
        f"sprout_social_api_url: {sprout_social_api_url}\n"
        f"yesterday: {yesterday}\n"
        f"sprout_social_secret_manager_id: {sprout_social_secret_manager_id}\n"
        f"red_shift_secret_manager_id: {red_shift_secret_manager_id}\n"
        f"output_bucket: {output_bucket}"
    )

    # BQ credentrial file encoded in base64
    sproute_social_secret = get_secrete_file(
        sprout_social_secret_manager_id, output_type="json"
    )

    red_shift_credential = get_secrete_file(
        red_shift_secret_manager_id, output_type="json"
    )



    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()

    if yesterday == "None":
        yesterday = (datetime.now() - timedelta(days=2)).date()
    else:
        yesterday = datetime.strptime(yesterday, "%Y-%m-%d").date()
    # Extract year, month, and day
    year_int = int(yesterday.year)
    month_int = int(yesterday.month)
    day_int = int(yesterday.day)

    created_time = str(yesterday) #2024-12-28

    print("created_time", created_time)

    # endregion

    s3_path = f"s3://{output_bucket}/{OUTPUT_BUCKET_KEY}/"


    print("All Code Runs Successfully")

    sagemaker_client.send_pipeline_execution_step_success(CallbackToken=token)
    print(f"KPI Agg Cell level completed successfully: Token={token}")

except Exception as e:
    print(f"Sagemaker client has stopped for Token={token}, the pipeline due to:  {e}")
    sagemaker_client.send_pipeline_execution_step_failure(CallbackToken=token)
    raise e
