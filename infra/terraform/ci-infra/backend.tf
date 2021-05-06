terraform {
  backend "gcs" {
    bucket  = "another-data-platform-tf"
    prefix  = ""
  }
}