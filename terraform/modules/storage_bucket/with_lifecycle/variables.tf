variable "bucket_name" {}
variable "location" {}
variable "labels" {}
variable "age"{
    type=number
    default= 1
    description = "days of conditiin"
}
variable "action"{
    type=string
    default="Delete"
    description = "action of lifecycle"
}

variable "public_access_prevention" {
    type=string
    default = "enforced"
}
