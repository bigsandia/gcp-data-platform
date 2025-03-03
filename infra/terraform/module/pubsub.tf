resource "google_pubsub_topic" "raw_data_buckets_notifications" {
  project = var.project_id
  name = "data-loader-raw-data-buckets-notifications"
}

resource "google_pubsub_topic" "data_loader_dead_letter" {
  project = var.project_id
  name = "data-loader-dead-letter-topic"
}

resource "google_pubsub_subscription" "data_loader_push" {
  project = var.project_id
  name = "data-loader"
  topic = google_pubsub_topic.raw_data_buckets_notifications.name

  push_config {
    push_endpoint = google_cloud_run_service.data_loader.status[0].url
    oidc_token {
      service_account_email = google_service_account.data_loader_invoker.email
    }
  }

  ack_deadline_seconds = 600

  dead_letter_policy {
    dead_letter_topic = google_pubsub_topic.data_loader_dead_letter.id
    max_delivery_attempts = 5
  }
}
