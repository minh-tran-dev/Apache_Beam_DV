resource "google_secret_manager_secret_iam_member" "secret" {
    secret_id = var.secret_id
    role = var.secret_role
    member = "serviceAccount:${var.service_account_mail}"
}
