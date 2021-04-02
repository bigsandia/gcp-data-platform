data "google_project" "raw_data_project" {
  for_each = var.raw_data_buckets_projects

  project_id = each.value
}

resource "google_pubsub_topic_iam_member" "enable_notifications_from_raw_data_projects" {
  for_each = data.google_project.raw_data_project

  project = var.project_id
  topic = google_pubsub_topic.raw_data_buckets_notifications.name
  role = "roles/pubsub.publisher"
  member = "serviceAccount:service-${each.value.number}@gs-project-accounts.iam.gserviceaccount.com"
}

resource "google_storage_notification" "raw_data_buckets_notifications" {
  for_each = var.raw_data_buckets

  bucket = each.value
  payload_format = "JSON_API_V1"
  topic = google_pubsub_topic.raw_data_buckets_notifications.id
  event_types = [
    "OBJECT_FINALIZE"]

  depends_on = [
    google_pubsub_topic_iam_member.enable_notifications_from_raw_data_projects]
}
