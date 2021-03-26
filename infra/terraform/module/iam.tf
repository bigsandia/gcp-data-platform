resource "google_project_iam_member" "pubsub_service_agent_token_creator" {
  project = var.project_id
  role = "roles/iam.serviceAccountTokenCreator"
  member = "serviceAccount:service-${data.google_project.current_project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "data_loader_bigquery_job_user" {
  project = var.project_id
  role = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.data_loader.email}"
}

resource "google_cloud_run_service_iam_member" "data_loader_invoker_run_invoker" {
  location = var.cloud_run_region
  project = var.project_id
  service = google_cloud_run_service.data_loader.name
  role = "roles/run.invoker"
  member = "serviceAccount:${google_service_account.data_loader_invoker.email}"
}
