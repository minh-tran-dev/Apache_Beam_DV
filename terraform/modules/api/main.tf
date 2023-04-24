resource "google_project_service" "dataflow-api" {
    project = var.project
    service = "dataflow.googleapis.com"
}
