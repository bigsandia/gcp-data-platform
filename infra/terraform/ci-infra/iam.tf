resource "google_project_iam_member" "cloudbuild_is_storage_admin" {
  project = data.google_project.project.name
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

resource "google_project_iam_member" "cloudbuild_is_bq_data_editor" {
  project = data.google_project.project.name
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}


resource "google_project_iam_member" "cloudbuild_is_bq_job_user" {
  project = data.google_project.project.name
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}
