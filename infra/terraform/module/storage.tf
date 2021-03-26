resource "google_storage_bucket" "data_loader_configuration" {
  project = var.project_id
  name = "${var.project_id}-data-loader-config"

  location = var.location
}

resource "google_storage_bucket_iam_member" "data_loader_configuration_object_viewer" {
  bucket = google_storage_bucket.data_loader_configuration.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.data_loader.email}"
}