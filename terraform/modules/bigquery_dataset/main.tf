resource "google_bigquery_dataset" "dataset"{
    dataset_id = var.dataset_id
    location = var.region
    labels = var.labels
}
