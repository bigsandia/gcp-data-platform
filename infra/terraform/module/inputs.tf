variable "project_id" {
  type = string
}

variable "cloud_run_region" {
  type = string
}

variable "data_loader_image_version" {
  type = string
  default = "latest"
}

variable "raw_data_buckets" {
  type = set(string)
}

variable "location" {
  type = string
  default = "US"
}