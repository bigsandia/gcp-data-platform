resource "google_storage_bucket" "data_loader_configuration" {
  project = var.project_id
  name = "${var.project_id}-data-loader-config"

  location = var.location
}
