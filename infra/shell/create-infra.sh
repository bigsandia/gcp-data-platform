#! /usr/bin/env sh

PROJECT_ID=another-data-platform
PROJECT_NUMBER=485415822394

gcloud config set project $PROJECT_ID

gsutil mb gs://$PROJECT_ID-raw-1

gcloud pubsub topics create storage-notif

gsutil notification create -f json -e OBJECT_FINALIZE -t projects/$PROJECT_ID/topics/storage-notif gs://another-data-platform-raw-1

TOPIC_NAME=storage-notif

gcloud pubsub topics create $TOPIC_NAME

#gcloud pubsub subscriptions create --topic=projects/$PROJECT_ID/topics/storage-notif

gcloud services enable run.googleapis.com

gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member=serviceAccount:service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com \
     --role=roles/iam.serviceAccountTokenCreator

gcloud iam service-accounts create data-loader-invoker \
     --display-name "Cloud Run Pub/Sub Invoker for data-loader"

gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member=serviceAccount:data-loader-invoker@$PROJECT_ID.iam.gserviceaccount.com \
     --role=roles/bigquery.jobUser

gcloud run services add-iam-policy-binding data-loader \
     --platform=managed \
     --region=europe-west1 \
     --member=serviceAccount:data-loader-invoker@$PROJECT_ID.iam.gserviceaccount.com \
     --role=roles/run.invoker

gcloud pubsub topics create data-loader-dead-letter-topic

gcloud pubsub subscriptions create data-loader --topic $TOPIC_NAME \
     --max-delivery-attempts=5 \
     --dead-letter-topic=data-loader-dead-letter-topic \
     --push-endpoint=https://data-loader-s3k5ketwpa-ew.a.run.app/ \
     --push-auth-service-account=data-loader-invoker@$PROJECT_ID.iam.gserviceaccount.com

# gcloud pubsub topics publish $TOPIC_NAME --message "JB + Ivan + Jojo = <3"

gsutil mb gs://$PROJECT_ID-config