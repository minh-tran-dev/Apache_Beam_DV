
=========================================
#   Run Example

### local

    python3 apache_beam_pipe.py \
        --dvpd imdb_dvpd.json \
        --mode local \
        --resources cimt-hh-resources \
        --runner DirectRunner \
        --project cimt-hh \
        --region europe-west1 \
        --temp_location gs://cimt-hh-temp-files/tmp/ \
        --staging_location gs://cimt-hh-resources/binaries/ \
        --job_name cimt-test \
        --no_db_operation

### gcp

    python3 apache_beam_pipe.py \
        --dvpd imdb_dvpd.json \
        --mode gcp \
        --resources cimt-hh-resources \
        --runner DataflowRunner \
        --project cimt-hh \
        --region europe-west1 \
        --temp_location gs://cimt-hh-temp-files/tmp/ \
        --staging_location gs://cimt-hh-resources/binaries/ \
        --job_name cimt-test \
        --save_main_session \
        --setup_file ./setup.py \
        --db_operation
    #    --service_account_email
    #    --network
    #    --subnetwork
    #    --no_use_public_ips

=========================================
# Notes


### Filtering

* add raw vault data back to pipeline
* execute cleanup sqls on target db at end

* beam sql
* dataframe

=========================================
# Issues
#### re-load into pipeline
* additional i/o workload
* big data incomatible

#### post sql process
* why not full ELT

#### beam sql
* java dependency
* multiple convert operations
* schema necessary

#### dataframe
* multiple convert operations
* schema necessary
* parallelism must be built extra
