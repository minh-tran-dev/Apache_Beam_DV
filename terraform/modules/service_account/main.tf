resource "google_service_account" "sa" {
    account_id = var.account_id
    display_name = var.display_name
    project = var.project
}

resource "google_project_iam_member" "iam" {
    project = var.project
    count = length(var.iam_roles)
    role = "roles/${var.iam_roles[count.index]}"
    member = "serviceAccount:${google_service_account.sa.email}"
}
