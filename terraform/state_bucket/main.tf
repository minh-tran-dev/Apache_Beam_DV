locals {
    project     = "cimt-hh"
    region      = "europe-west1"
}

provider "google" {
    project = local.project
    region = local.region
}

resource "google_storage_bucket" "state" {
    name = "${local.project}-tf-state"
    location = local.region
    uniform_bucket_level_access = true

    versioning { enabled = true}
    lifecycle_rule {
        condition { num_newer_versions = 4}
        action { type = "Delete"}
    }
    lifecycle { prevent_destroy = true}
    labels = { service = "state-bucket"}
}
