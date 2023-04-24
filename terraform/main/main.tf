

resource "google_storage_bucket_object" "imdb_dvpd"{
    name = "dvpd/imdb_dvpd.json"
    source = "../../resources/dvpd/imdb_dvpd.json"
    bucket = local.resource_bucket
}


resource "google_storage_bucket_object" "pipeline_config"{
    name = "config/pipeline_conf.json"
    source = "../../resources/config/pipeline_conf.json"
    bucket = local.resource_bucket
}

resource "google_storage_bucket_object" "logging_config"{
    name = "config/logger.conf"
    source = "../../config/logger.conf"
    bucket = local.resource_bucket
}

resource "google_storage_bucket_object" "imdb_data"{
    name = "input/test_data2.csv"
    source = "../../resources/csv/test_data2.csv"
    bucket = local.resource_bucket
}
