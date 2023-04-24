from typing import Iterable, Dict, List, Any
import argparse, logging, time, datetime
from logging import config
import apache_beam as beam

# import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert

# from apache_beam.transforms.sql import SqlTransform

import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

from handlers.data_struct.dvpd import Dvpd
import handlers.database.database_factory as db_factory
import handlers.file.file_handler_factory as file_factory
import helpers.md5_gen as hash

logger = None


class GetTimestamp(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        yield timestamp.to_utc_datetime()


def setup_parser(argv=None) -> tuple:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dvpd", dest="dvpd", required=True, type=str, help="insert_dvpd_file_name"
    )
    parser.add_argument(
        "--delete",
        dest="delete",
        required=False,
        action="store_true",
        help="use delete or no_delete",
    )
    parser.add_argument(
        "--no_delete",
        dest="delete",
        required=False,
        action="store_false",
        help="use delete or no_delete",
    )
    parser.add_argument(
        "--db_operation",
        dest="db_operation",
        required=False,
        action="store_true",
        help="use db_operation or no_db_operation",
    )
    parser.add_argument(
        "--no_db_operation",
        dest="db_operation",
        required=False,
        action="store_false",
        help="use db_operation or no_db_operation",
    )
    parser.add_argument(
        "--resources",
        dest="resources",
        required=False,
        type=str,
        default=None,
        help="main resource path",
    )
    parser.add_argument(
        "--mode", dest="mode", required=True, type=str, help="pipeline execution mode"
    )

    parser.set_defaults(delete=False)
    parser.set_defaults(db_operation=True)

    return parser.parse_known_args(argv)


def get_db_handler_factory(handler_type: str) -> db_factory.DataBaseHandlerFactory:
    dbs = {"bigquery": db_factory.GcpFactory()}
    return dbs[handler_type]


def get_file_handler_factory(handler_type: str) -> file_factory.FileHandlerFactory:
    fh_factories = {
        "local": file_factory.LocalFileFactory(),
        "gcp": file_factory.GcpFileFactory(),
    }
    return fh_factories[handler_type]


def add_hash_col(
    row: beam.Dict, hash_column_name: str, columns: Iterable, delimiter: str = "|"
) -> Dict:
    if hash_values := list(map(lambda field_col: row[field_col], columns)):
        row[hash_column_name] = hash.get_md5(hash_values)
    return row


# !!!----------- TO-DO  : remove hard coded selection
def add_hash_diff(row: beam.Dict, dv_obj: str, col_name: str) -> Dict:
    if "sat" not in dv_obj:
        return row
    field_list = list(row.values())
    row[col_name] = hash.get_md5(field_list[1:-3])
    return row


def get_hash_fields(
    target_table: str, dvpd_models: List[Dict], res: tuple = ()
) -> tuple:
    """recursive function to retrieve all values to be hashed"""
    res = ()
    for dv_model in dvpd_models:
        for table in dv_model["tables"]:
            if table["table_name"] == target_table:
                if table["stereotype"] == "hub":
                    res += tuple(table["hkey"])
                elif table["stereotype"] == "sat":
                    res = get_hash_fields(table["parent_table"], dvpd_models, res=res)
                elif table["stereotype"] == "link":
                    for parent_table in table["link_objects"]:
                        res = get_hash_fields(parent_table, dvpd_models, res=res)
    return res


def add_hkey(row: beam.Dict, dvpd_models: List[Dict]) -> Dict:
    for dv_model in dvpd_models:
        for table in dv_model.get("tables"):
            row = add_hash_col(
                row=row,
                hash_column_name=table["key_column_name"],
                columns=get_hash_fields(
                    target_table=table["table_name"], dvpd_models=dvpd_models
                ),
            )
    return row


def add_column(row: beam.Dict, col_name: str, col_value: Any) -> Dict:
    row[col_name] = col_value
    return row


def add_timestamp(row: beam.Dict, project_conf: Dict) -> beam.Dict:
    dt = datetime.datetime.now(datetime.timezone.utc)
    row[project_conf["technical_fields"]["ldts"]["name"]] = dt.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    return row


def get_tech_field_columns(result: List[str], tech_fields: Dict) -> List[str]:
    result.extend(t_field.get("name") for t_field in tech_fields.values())
    return result


def filter_by_key(row: beam.Dict, args: List[str]):
    return {x: row[x] for x in args if x in row.keys()}


def split_fields(row: beam.Dict, table: str, dvpd: Dvpd, tech_fields: Dict) -> Dict:
    field_list = []
    field_list += dvpd.get_hkey_cols_for_table(table=table, result=field_list)
    field_list += dvpd.get_field_cols_for_table(table=table, result=field_list)
    field_list += get_tech_field_columns(result=field_list, tech_fields=tech_fields)
    return filter_by_key(row=row, args=field_list)


def hub_filter_pattern(row: beam.Dict, side_input: List[Dict]) -> Dict:
    return row


def filter_inserts(row: beam.Dict, side_input: List[Dict], dv_type: str) -> Dict:
    # logger.info(side_input)
    logger.info(dv_type)
    filter_pattern = {"hub": hub_filter_pattern}
    row = filter_pattern[dv_type](row, side_input)
    return row


def run(argv=None, save_main_session: bool = True) -> None:
    known_args, pipeline_args = setup_parser()

    pipeline_options = PipelineOptions(  # Object of apachebeam
        pipeline_args, save_main_session=True
    )

    p = beam.Pipeline(
        argv=pipeline_args, options=pipeline_options
    )  # ein Pipeline of ApacheBeam

    fm_factory: file_factory.FileHandlerFactory = get_file_handler_factory(
        handler_type=known_args.mode
    )
    fm = fm_factory.get_file_handler()  # is FileHandler Object.

    # -------------------- setup logging

    logger_conf = fm.get_conf_file(path=known_args.resources, config_name="logger.conf")
    with logger_conf.open("r") as f:
        config.fileConfig(f)  # defaults={ 'logfilename' : 'logs/beam.log' })

    logger = logging.getLogger(__name__)

    if known_args.mode == "gcp":
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client)
        setup_logging(handler)

    logger.info("Start Pipeline")

    # ---------- load pipeline files & configs

    dvpd = fm.read_dvpd(
        known_args.dvpd, known_args.resources
    )  # ein dvpd object in Pydantic Modell
    project_conf = fm.get_pipeline_conf(known_args.resources)  # ein dict
    dv_models = dvpd.dict(include={"data_vault_model"}).get("data_vault_model")

    logger.info("DVPD Read")

    # ----------- Setup destination
    # !!! TO-DO Check for changes before execution

    if known_args.db_operation:
        database_factory: db_factory.DataBaseHandlerFactory = get_db_handler_factory(
            handler_type=dvpd.data_destination.database.lower()
        )

        db_handler = database_factory.get_db_handler(dvpd=dvpd)

        db_handler.create_ddls(tech_fields=project_conf.get("technical_fields"))

        db_handler.create_tables(
            delete=known_args.delete,
            partition=project_conf.get("technical_fields").get("ldts").get("name"),
        )

    # ----------- Actual start of pipeline
    # dvpd_data = p | beam.Create(dvpd.dict(include={'fields','data_vault_model'}).items()) | beam.Map(print)

    source_data = p | "Read Data Source" >> beam.dataframe.io.read_csv(
        fm.get_file_path(dvpd, known_args.resources)
    )
    src_data_as_dict = convert.to_pcollection(
        source_data
    ) | "convert to dict" >> beam.Map(
        lambda x: dict(x._asdict())
    )  # ein Dict PCollection

    added_tech_fields = (
        src_data_as_dict
        # | 'apply hard rules'
        | "add hash columns" >> beam.Map(add_hkey, dv_models)
        | "add Timestamps"
        >> beam.Map(lambda row: beam.window.TimestampedValue(row, time.time()))
        | "add Timestamps to pcol" >> beam.Map(add_timestamp, project_conf)
        | "add record source"
        >> beam.Map(
            add_column,
            project_conf["technical_fields"]["rsrc"]["name"],
            dvpd.record_source_name_expression,
        )
        | "add meta is deleted"
        >> beam.Map(
            add_column, project_conf["technical_fields"]["is_deleted"]["name"], False
        )
        | "add job instance id"
        >> beam.Map(
            add_column,
            project_conf["technical_fields"]["job_instance"]["name"],
            int(time.time()),
        )
        # | 'debug time' >> beam.ParDo(GetTimestamp())
        # | 'debug print' >> beam.Map(print)
    )

    dv_obj_list = dvpd.create_dv_obj_list()
    dv_obj_list = [{"schema": "rvlt_test", "table": "rkggl_movie_hub"}]

    # if dvpd.data_destination.database.lower() == 'bigquery':
    #    db_data = {dv_obj["table"]:(
    #        p
    #        | f'Read BQ Table :{dv_obj["table"]}' >> beam.io.ReadFromBigQuery(table=f'{dvpd.data_destination.project}.{dv_obj["schema"]}.{dv_obj["table"]}')
    #    ) for dv_obj in dv_obj_list
    #    }

    transformed = {
        dv_obj["table"]: (
            added_tech_fields
            | f'Split Fields {dv_obj["table"]}'
            >> beam.Map(
                split_fields, dv_obj["table"], dvpd, project_conf["technical_fields"]
            )
            | f'Add HDiff in Sats {dv_obj["table"]}'
            >> beam.Map(
                add_hash_diff,
                dv_obj["table"],
                project_conf["technical_fields"]["hdiff"]["name"],
            )
            # | f'change to Beam Rows {dv_obj["table"]}' >> beam.Map(lambda row:beam.Row(**row))
            # | "15s fixed windows" >> beam.WindowInto(beam.window.FixedWindows(15))
            # | f'Filter Duplicates {dv_obj["table"]}' >> SqlTransform(query=query)
            # | f'Filter valid Source entries {dv_obj["table"]}' >>
            # | f'convert to dict {dv_obj["table"]}' >> beam.Map(lambda row : row.as_dict())
            | f'Select {dv_obj["table"]}'
            >> beam.Select(
                hk_rkggl_movie=lambda x: str(x["hk_rkggl_movie"]),
                name=lambda x: str(x["name"]),
                LDTS=lambda x: str(x["LDTS"]),
                RSRC=lambda x: str(x["RSRC"]),
                META_IS_DELETED=lambda x: bool(x["META_IS_DELETED"]),
                META_JOB_INSTANCE_ID=lambda x: int(x["META_JOB_INSTANCE_ID"]),
            )
        )
        for dv_obj in dv_obj_list
    }

    df_list: List = [
        convert.to_dataframe(transformed[dv_obj["table"]]) for dv_obj in dv_obj_list
    ]

    p_col_list: List[beam.PCollection] = [
        convert.to_pcollection(df, include_indexes=False) for df in df_list
    ]

    testing = [(p_col | beam.Map(print)) for p_col in p_col_list]

    # test = convert.to_pcollection(df_list[0],include_indexes=False)

    # blub = test |beam.Map(print)

    # if dvpd.data_destination.database.lower() == 'bigquery':
    #    total_elements = [(
    #        p_col
    #        #| f'testing {dv_obj}' >> beam.Map(filter_inserts,beam.pvalue.AsList(db_data[dv_obj]),'hub')
    #        #| f'Write Object {dv_obj}' >> beam.io.WriteToBigQuery(
    #        #    table=f'{dvpd.data_destination.project}.{dvpd.get_table_schema(dv_obj)}.{dv_obj}',
    #        #    schema='SCHEMA_AUTODETECT',
    #        #    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #        #    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    #        #)
    #        | f'debug print {dv_obj}' >> beam.Map(print)
    #    ) for dv_obj,p_col in transformed.items()
    #    ]

    p.run().wait_until_finish()
    logger.info("END - Pipeline")


if __name__ == "__main__":
    run()
