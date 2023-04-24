module "resource-bucket" {
    source = "../modules/storage_bucket"
    bucket_name = "${local.project}-resources"
    location = local.region
    labels = null
}

module "temp-file-bucket" {
    source = "../modules/storage_bucket/with_lifecycle"
    bucket_name = "${local.project}-temp-files"
    location = local.region
    labels = null
    action = "Delete"
    age = 1

}
