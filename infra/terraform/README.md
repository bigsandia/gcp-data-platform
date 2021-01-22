# Prérequis

Doit être activée :

- Cloud Resource Manager
  API : https://console.cloud.google.com/marketplace/product/google/cloudresourcemanager.googleapis.com
- Identity and Access Management
  API : https://console.cloud.google.com/marketplace/product/google/iam.googleapis.com

Rôle de la personne utilisant le module :

- Editor
- Project IAM Admin
- Pub/Sub Admin (pour enable_notifications)
- Cloud Run Admin (google_cloud_run_service_iam_binding.data_loader_invoker_run_invoker)