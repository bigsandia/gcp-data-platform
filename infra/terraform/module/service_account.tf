resource "google_service_account" "data_loader" {
  project = var.project_id
  account_id = "data-loader"
  description = "Service account for data-loader"
}

resource "google_service_account" "data_loader_invoker" {
  project = var.project_id
  account_id = "data-loader-invoker"
  description = "Cloud Run Pub/Sub Invoker for data-loader"
}
