resource "google_bigquery_table" "table" {
    dataset_id = var.dataset_id
    table_id = var.table_id
    schema = var.schema_file_path
    labels = var.labels

}
