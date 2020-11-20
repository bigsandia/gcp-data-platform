#! /usr/bin/env sh

gsutil mb gs://another-data-platform-raw-1

gcloud pubsub topics create storage-notif

gsutil notification create -f json -e OBJECT_FINALIZE -t projects/another-data-platform/topics/storage-notif gs://another-data-platform-raw-1

gcloud pubsub topics create storage-notif

#gcloud pubsub subscriptions create --topic=projects/another-data-platform/topics/storage-notif

gcloud services enable run.googleapis.com