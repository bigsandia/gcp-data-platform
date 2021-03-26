module "gcp-data-platform" {
  source = "../module"

  project_id = "bigsandia-17956e"
  cloud_run_region = "europe-west1"
  raw_data_buckets = [
    "bigsandia-17956e-raw-data-1",
    "bigsandia-17956e-raw-data-2"
  ]
  raw_data_buckets_projects = [
    "bigsandia-17956e"
  ]

  location = "US"
  data_loader_image = "eu.gcr.io/another-data-platform/run/data-loader:685d104"
}
