#!/bin/bash

## The script was not tested due to expiring GCP trial.
## It commands for deploying Cloud Functions (for triggering DataFlow jobs), 
## PubSub topic (for triggering one of Cloud Functions),
## DataFlow templates,
## Cloud SQL instance and its database.

## !! The script requires Cloud Functions and DataFlow templates projects to be in the same directory. !!

PROJECT="phase-one-322509"

PUBSUB_TOPIC_NAME="topic1"
REGION="us-central1"

EXTRACT_JOB_TRIGGER_FUNCTION_NAME="extract-job-trigger"
EXTRACT_JOB_TRIGGER_FUNCTION_ENTRY_POINT="com.drobot.cfunc.trigger.ExtractJobTrigger.java"
EXTRACT_JOB_TRIGGER_FUNCTION_MEMORY="512MB"

TRANSFORM_JOB_TRIGGER_FUNCTION_NAME="transform-job-trigger"
TRANSFORM_JOB_TRIGGER_FUNCTION_ENTRY_POINT="com.drobot.cfunc.trigger.TransformJobTrigger.java"
TRANSFORM_JOB_TRIGGER_FUNCTION_MEMORY="512MB"

DATAFLOW_STAGING_LOCATION="gs://fdx-training-1/staging"
DATAFLOW_TEMPLATES_LOCATION="gs://fdx-training-1/templates"

CLOUD_SQL_TO_GCS_MAIN_CLASS="com.drobot.beam.Main"
CLOUD_SQL_TO_GCS_TEMPLATE_NAME="cloudSql-to-gcs"

AVRO_TO_BQ_MAIN_CLASS="com.drobot.beam.Main"
AVRO_TO_BQ_TEMPLATE_NAME="avro-to-bq-with-aggregation"

CLOUD_SQL_INSTANCE_NAME="first-instance"
CLOUD_SQL_INSTANCE_CPU="4"
CLOUD_SQL_INSTANCE_MEMORY="4GB"
CLOUD_SQL_INSTANCE_VERSION="POSTGRES_13"
CLOUD_SQL_INSTANCE_STORAGE_TYPE="SSD"
CLOUD_SQL_INSTANCE_AVAILABILITY_TYPE="ZONAL"
CLOUD_SQL_INSTANCE_PASSWORD="postgres"
CLOUD_SQL_INSTANCE_DATABASE_NAME="custom"

## Changing directory to bash script's ones
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

## Choosing neccessary Google Cloud project
gcloud config set project $PROJECT

## Creating PubSub topic for triggering Cloud Function
gcloud pubsub topics create $PUBSUB_TOPIC_NAME

## Change directory to the Cloud Functions project
cd cloud-function

## Deploying Cloud Function for triggering cloudSql-to-gcs DataFlow job
gcloud functions deploy $EXTRACT_JOB_TRIGGER_FUNCTION_NAME \
--entry-point $EXTRACT_JOB_TRIGGER_FUNCTION_ENTRY_POINT \
--runtime java11 \
--memory $EXTRACT_JOB_TRIGGER_FUNCTION_MEMORY \
--trigger-topic $PUBSUB_TOPIC_NAME

## Deploying Cloud Function for triggering avro-to-bq-with-aggregation DataFlow job
gcloud functions deploy $TRANSFORM_JOB_TRIGGER_FUNCTION_NAME \
--entry-point $TRANSFORM_JOB_TRIGGER_FUNCTION_ENTRY_POINT \
--runtime java11 \
--memory $TRANSFORM_JOB_TRIGGER_FUNCTION_MEMORY \
--trigger-http \
--allow-unauthenticated

## Change directory to cloudSql-to-gcs project
cd ../postgres-to-gcs

## Creating $CLOUD_SQL_TO_GCS_TEMPLATE_NAME DataFlow template
mvn compile exec:java \
    -Dexec.mainClass=$CLOUD_SQL_TO_GCS_MAIN_CLASS -Dexec.args=\"--runner=DataflowRunner --project=$PROJECT --stagingLocation=$DATAFLOW_STAGING_LOCATION --templateLocation=$DATAFLOW_TEMPLATES_LOCATION/$CLOUD_SQL_TO_GCS_TEMPLATE_NAME --region=$REGION\"

## Change directory to avro-to-bq-with-aggregation project DataFlow template
cd ../avro-to-bigquery-with-transform

## Creating $AVRO_TO_BQ_TEMPLATE_NAME DataFlow template
mvn compile exec:java -Dexec.mainClass=$CLOUD_SQL_TO_GCS_MAIN_CLASS \
    -Dexec.args=\"--runner=DataflowRunner --project=$PROJECT --stagingLocation=$DATAFLOW_STAGING_LOCATION --templateLocation=$DATAFLOW_TEMPLATES_LOCATION/$AVRO_TO_BQ_TEMPLATE_NAME --region=$REGION\"

cd ..

## Creating Cloud SQL instance
gcloud sql instances create $CLOUD_SQL_INSTANCE_NAME \
--cpu=$CLOUD_SQL_INSTANCE_CPU \
--memory=$CLOUD_SQL_INSTANCE_MEMORY \
--region=$REGION \
--database-version=$CLOUD_SQL_INSTANCE_VERSION \
--authorized-networks \
--storage-type=$CLOUD_SQL_INSTANCE_STORAGE_TYPE \
--availability-type=$CLOUD_SQL_INSTANCE_AVAILABILITY_TYPE 

gcloud sql users set-password postgres \
--instance=$CLOUD_SQL_INSTANCE_NAME \
--password=$CLOUD_SQL_INSTANCE_PASSWORD

gcloud sql databases create $CLOUD_SQL_INSTANCE_DATABASE_NAME \
--instance=$CLOUD_SQL_INSTANCE_NAME
