"""
 Copyright 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

Description: Example of pre-processing code using a PySpark processor
             This file presents code examples for:
              * read parameters from the processing job
              * read parquet data from s3
              * save data to s3
              * set logs to be displayed with the start of the job execution
              * use extra helper python files
              * Add extra parameters to SageMaker Experiments
"""

# import requirements
import argparse
import logging
import sys
import os
import pandas as pd
import csv
import shutil
import time

# spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, FloatType

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)

# Define custom handler
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def extract(row):
    return (row[0],) + tuple(row[1].toArray().tolist())

def main(data_path):
    
    print("Starting SparkSession")

    spark = SparkSession.builder.appName("PySparkJob").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter"
    )

    schema = StructType(
        [
            StructField("sex", StringType(), True),
            StructField("length", DoubleType(), True),
            StructField("diameter", DoubleType(), True),
            StructField("height", DoubleType(), True),
            StructField("whole_weight", DoubleType(), True),
            StructField("shucked_weight", DoubleType(), True),
            StructField("viscera_weight", DoubleType(), True),
            StructField("shell_weight", DoubleType(), True),
            StructField("rings", DoubleType(), True),
        ]
    )
    print(f"Reading CSV at {data_path}")
    total_df = spark.read.csv(data_path, header=False, schema=schema)
    print(f"Successfully read")
    # StringIndexer on the sex column which has categorical value
    sex_indexer = StringIndexer(inputCol="sex", outputCol="indexed_sex")

    # one-hot-encoding is being performed on the string-indexed sex column (indexed_sex)
    sex_encoder = OneHotEncoder(inputCol="indexed_sex", outputCol="sex_vec")

    # vector-assembler will bring all the features to a 1D vector for us to save easily into CSV format
    assembler = VectorAssembler(
        inputCols=[
            "sex_vec",
            "length",
            "diameter",
            "height",
            "whole_weight",
            "shucked_weight",
            "viscera_weight",
            "shell_weight",
        ],
        outputCol="features",
    )

    # The pipeline is comprised of the steps added above
    pipeline = Pipeline(stages=[sex_indexer, sex_encoder, assembler])
    
    print(f"Running through pipeline")

    # This step trains the feature transformers - this takes a while to run
    model = pipeline.fit(total_df)

    # This step transforms the dataset with information obtained from the previous fit
    transformed_total_df = model.transform(total_df)
    
    print("Transforming Data")
    # Convert the transformed dataframe to RDD to save in CSV format and upload to S3
    df = transformed_total_df.rdd.map(lambda x: (x.rings, x.features))
    df_return = df.map(extract).toDF()
    
    return df_return

if __name__ == "__main__":
    logger.info(f"===============================================================")
    logger.info(f"================= Starting pyspark-processing =================")
    parser = argparse.ArgumentParser(description="app inputs")
    parser.add_argument("--input-table", type=str, help="path to the channel data")
    parser.add_argument("--output-table", type=str, help="path to the output data")
    args = parser.parse_args()
    logger.info(f"================= Starting Spark Application =================")
    df = main(args.input_table)

    logger.info("Writing transformed data")
    df.write.csv(os.path.join(args.output_table, "transformed.csv"), header=True, mode="overwrite")

    logger.info(f"================== Ending pyspark-processing ==================")
    logger.info(f"===============================================================")