module "gcp-data-platform" {
  source = "../module"

  project_id = "bigsandia-17956e"
  cloud_run_region = "europe-west1"
  raw_data_buckets = [
    "bigsandia-17956e-raw-data-1",
    "bigsandia-17956e-raw-data-2"
  ]

  location = "US"
  data_loader_image_version = "685d104"
}
