# Prérequis

## APIs à activer

- Cloud Resource Manager
  API : https://console.cloud.google.com/marketplace/product/google/cloudresourcemanager.googleapis.com
- Identity and Access Management
  API : https://console.cloud.google.com/marketplace/product/google/iam.googleapis.com
- PubSub
- Cloud Run
- Storage

## Rôle de la personne utilisant le module

- Editor
- Project IAM Admin
- Pub/Sub Admin (pour enable_notifications)
- Cloud Run Admin (google_cloud_run_service_iam_binding.data_loader_invoker_run_invoker)
- storage.buckets.get + storage.buckets.update sur les buckets qui contiennent la donnée brute,
  sinon :

```
Error: Error creating notification config for bucket <bucket_name_raw_data>: googleapi: Error 403: <my_service_account> does not have storage.buckets.get access to the Google Cloud Storage bucket., forbidden
```

Si erreur :

```
Error 400: Service account service-xxx@gs-project-accounts.iam.gserviceaccount.com does not exist.
```

https://cloud.google.com/storage/docs/getting-service-agent#console
Console Storage > Settings > le service account apparaît

On doit l'activer pour tous les projets qui contiennent la donnée brute.

## IAM pour data-loader

Le service account data-loader doit avoir les droits suivants :

- storage viewer sur les buckets sources
- bigquery admin sur le projet BQ destination
