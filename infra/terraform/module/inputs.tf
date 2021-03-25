variable "project_id" {
  type = string
}

variable "cloud_run_region" {
  type = string
}

variable "data_loader_image" {
  type = string
}

variable "raw_data_buckets" {
  type = set(string)
}

variable "location" {
  type = string
  default = "US"
}