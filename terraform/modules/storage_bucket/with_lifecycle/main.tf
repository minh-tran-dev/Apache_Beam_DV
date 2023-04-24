resource "google_storage_bucket" "bucket" {
    name = var.bucket_name
    location =  var.location
    uniform_bucket_level_access = true
    labels = var.labels
    public_access_prevention = var.public_access_prevention
    lifecycle_rule {
        condition {
            age = var.age
        }
        action {
            type = var.action
        }
    }
}
