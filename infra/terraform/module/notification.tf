resource "google_pubsub_topic_iam_binding" "enable_notifications" {
  project = var.project_id
  topic = google_pubsub_topic.raw_data_buckets_notifications.name
  role = "roles/pubsub.publisher"
  members = [
    "serviceAccount:service-${data.google_project.current_project.number}@gs-project-accounts.iam.gserviceaccount.com"
  ]
}

resource "google_storage_notification" "raw_data_buckets_notifications" {
  for_each = var.raw_data_buckets

  bucket = each.value
  payload_format = "JSON_API_V1"
  topic = google_pubsub_topic.raw_data_buckets_notifications.id
  event_types = [
    "OBJECT_FINALIZE"]

  depends_on = [
    google_pubsub_topic_iam_binding.enable_notifications]
}
