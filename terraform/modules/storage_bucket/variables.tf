variable "bucket_name" {}
variable "location" {}
variable "labels" {}
variable "public_access_prevention" {
    type=string
    default = "enforced"
}
