provider "google" {
    project = local.project
    region = local.region
}

terraform {
    backend "gcs" {
        bucket = "cimt-hh-tf-state"
        prefix = "basic_infra"
    }
    required_providers {
        google = {
            source  = "hashicorp/google"
            version = "~> 4.43.0"
        }
        archive = {
            source  = "hashicorp/archive"
            version = "~> 2.0.0"
        }
    }
}
