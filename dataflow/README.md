# To build and run Pub/Sub to GCS Flex Template

# Prerequisites

Before running the Flex Template pipeline, make sure you have following prerequisites

- A Google Cloud Pub/Sub topic and subscription in Source Project
- A Google Cloud Storage bucket in Landing Project

# Required Parameters

Before running the Flex Template, you'll need to gather the following required parameters:

1. **Pub/Sub Subscription**
   
   Name of your Pub/Sub Subscription.Make sure it is in the below format.
   "projects/<project_id>/subscriptions/<subscription_name>"  

2. **GCS Bucket**
   
   Provide the GCS bucket path to store output files.Make sure it is in the below format.
   "gs://<bucket_name>/<source_name_json>/"

3. **Pipeline Name**
   
   Provide the name of the streaming pipeline. Make sure it is in the format.
   "hca-streaming-<source_name>-json" 

# Optional Parameters

1. **Window size**

   Default window size for pipeline is 10 minutes.For every 10 minutes output files will be generated in Output path

2. **Number of Shards**
   
   Provide no. of shards you want for a window. Default num_shards is 1.



# To build Docker Image

Export Environment Variables     

```
export PROJECT="hca-usr-hin-datalake-poc"
export REGION="us-central1"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow-pubsub-to-gcs:latest"
export TEMPLATE_PATH="gs://hca_job_data_20231009/dataflow/dataflow-flex-templates/Json_pubsub_to_gcs"

```

Run below command

`gcloud builds submit --tag $TEMPLATE_IMAGE --gcs-log-dir "gs://hca_job_data_20231009/cloud_build/logs/" `

# To Build Flex-template

Run below command

```
gcloud dataflow flex-template build $TEMPLATE_PATH \
--image "$TEMPLATE_IMAGE"  \
--sdk-language "PYTHON"  \
--metadata-file "metadata.json"

```

# Run the pipeline
Replace below variables with your values 

```
export PIPELINE_NAME="hca-streaming-employee-competencies-json" 
export INPUT_SUBSCRIPTION ="projects/hca-usr-hin-datalake-poc/subscriptions/hca_employee_competencies_json-sub"
export OUTPUT_PATH="gs://hca_employee-data_landing_20231005/employee_competencies_json/"
export WINDOW_SIZE=10 
export NUM_SHARDS=1

```  

```
gcloud dataflow flex-template run $PIPELINE_NAME \
 --template-file-gcs-location "$TEMPLATE_PATH" \
 --parameters input_subscription=$INPUT_SUBSCRIPTION \
 --parameters output_path=$OUTPUT_PATH \
 --parameters window_size=$WINDOW_SIZE \
 --parameters num_shards=$NUM_SHARDS \
 --region $REGION \
 --temp-location "gs://hca_job_data_20231009/dataflow/temp" \
 --staging-location "gs://hca_job_data_20231009/dataflow/staging" \
 --network "hca-datalake-processing-net" \
 --disable-public-ips \
 --service-account-email "hca-dataflow-datalake-poc-sa@hca-usr-hin-proc-datalake.iam.gserviceaccount.com" 

 ```