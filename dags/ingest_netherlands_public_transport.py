import logging
import json

from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from infrastructure.connectors.ovapi.client import OVAPI
from domain.entities.public_transports.netherlands.lines import LinesRecords


logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 1 * * *",
    start_date=datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
)
def ingest_netherlands_public_transport():
    s3_client = S3Hook("conn_aws").get_conn()

    @task()
    def get_lines_data():
        return LinesRecords(ovapi_client=OVAPI()).csv_dumps()

    @task()
    def upload_data_to_s3(data, directory, data_filename):
        context = get_current_context()

        execution_date = context["execution_date"]
        filename = "_".join([data_filename, str(execution_date)]) + ".csv"

        s3_client.upload_file(filename, directory, data).upload_data()

        manifest_filename = s3_client.upload_manifest(
            directory, filenames=filename, date=execution_date
        )

        return json.dumps(
            {"files_list": [filename], "file_manifest": manifest_filename}
        )

    @task()
    def load_data_to_snowflake(data, schema, directory, table_name):

        context = get_current_context()

        content = json.loads(data)

        logger.info("load_data: push data to database")

        S3ToSnowflakeOperator(
            schema=schema,
            directory=directory,
            table=table_name,
            context=context,
            copy_options="(type=CSV, field_delimiter=';')",
            files_lifiles_listst=content["files_list"],
            file_manifest=content["file_manifest"],
        )

    @task_group()
    def ingest_lines():
        data = get_lines_data()
        manifest = upload_data_to_s3(
            data, directory="raw_ingest", data_filename="lines"
        )
        loader_task = load_data_to_snowflake(
            manifest, schema="raw_db", directory="raw_ingest", table_name="raw_lines"
        )

    ingest_lines()


dag = ingest_netherlands_public_transport()
