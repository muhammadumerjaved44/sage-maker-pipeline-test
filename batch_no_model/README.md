# Batch Inference Deployment

### Repository structure

```
|-- cloud_formation
|   |-- batch-config-template.yml
|   |-- pre-prod-config.json
|   |-- prod-config.json
|-- pipelines
|   |-- run_pipeline.py
|   |-- get_pipeline_definition.py
|   |-- _utils.py
|   |-- _version_.py
|   |-- _init_.py
|   |-- README.md
|   |-- batch_inference
|   |   |-- README.md
|   |   |-- pipeline.py
|   |   |-- preprocess.py
|-- build.py
|-- utils.py
```

A description of some of the artifacts is provided below.

```
|-- cloud_formation
|   |-- batch-config-template.yml
```
-  This template is built and deployed by the infrastructure pipeline in various stages (pre-production/production) as required for Batch inference.
  It specifies the resources that need to be created, like the SageMaker Model. It can be extended to include resources as required.


```
|-- cloud_formation
|   |-- pre-prod-config.json
```

- this configuration file is used to customize `staging` stage in the pipeline. You can configure the instance type, instance count, schedule expression here.

```
|-- cloud_formation
|   |-- prod-config.json
```

- this configuration file is used to customize `prod` stage in the pipeline. You can configure the instance type, instance count, schedule expression here.


```
|-- build.py
```

- This python file contains code to get the latest approved model version of the project. Then, checks if the version is already in the target account, in case is not, it uploads the model artifact for that version in the S3 created with the base infra and creates the Model Package in the target account.
The parameters set in the build.py are added into de config.json for the target account. They are used as input parameters into the CloudFormation template.
It is also created the sagemaker pipeline definition from get_pipeline_definition and uploaded to `pipelinedefinition.json`. This pipeline definition is used for creating the pipeline in CloudFormation.


```
|-- utils.py
```

- This file contains the necessary functions for build.py

```
|-- batch_inference
```
- This folder contains SageMaker Pipeline definitions and helper scripts to either simply "get" a SageMaker Pipeline definition (JSON dictionnary) with `get_pipeline_definition.py`, or "run" a SageMaker Pipeline from a SageMaker pipeline definition with `run_pipeline.py`.

Each SageMaker Pipeline definition should be be treated as a module inside its own folder, for example here the "batch_inference" pipeline, contained inside `batch_inference/`.

This SageMaker Pipeline definition creates a workflow that will:
- Prepare the inference dataset through a SageMaker Processing Job
- Run the inference with a Batch transform job
