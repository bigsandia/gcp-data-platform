resource "google_cloud_run_service" "data_loader" {
  project = var.project_id
  name = "data-loader"
  location = var.cloud_run_region

  template {
    metadata {
      annotations = {
        "run.googleapis.com/sandbox" = "gvisor"
        "autoscaling.knative.dev/maxScale" = "1"
      }
    }

    spec {
      containers {
        image = var.data_loader_image
        env {
          name = "CONFIG_BUCKET_NAME"
          value = google_storage_bucket.data_loader_configuration.name
        }
        env {
          name = "DATA_LOCATION"
          value = var.location
        }
      }

      container_concurrency = 50
      service_account_name = google_service_account.data_loader.email
    }
  }

  traffic {
    percent = 100
    latest_revision = true
  }

  autogenerate_revision_name = true
}
