resource "google_storage_bucket_object" "files" {
    count = length(var.file_names)
    bucket = var.bucket_name
    name = var.file_names[count.index]
    source = var.file_paths[count.index]
}
