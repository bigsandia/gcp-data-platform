terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.52.0"
    }
  }
}

provider "google" {
  region = "europe-west1"
}
