import io
import pandas as pd
from pandas import DataFrame, Timestamp
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build


class Loader:
    """This represent a loader of data of different services of google cloud computing.

    Attributes:
        project_id: The id of project in GCP.
        bucket_name: Name of the bucket where download the data.
        storage_client: Client for API cloud storage GCP.
        bigquery_client: Client for API cloud bigquery GCP.
        googlesheet_client: Client for API  Google Sheets.
        credentials: Object for authentication.

    Methods:
        getAllBlobs(): List all BLOBS in bucket.
        downloadBucket(): Download a BLOB type Excel to DataFrame.
        downloadSheets(): Download data from Google Sheets to DataFrame.
        uploadBigQuery(): Upload a DataFrame to table in BigQuery.
        cleanRaw(): Trigger a store procedure that clean data raw for add new data.
    """

    def __init__(self) -> None:
        self.project_id = "bancobogotaprueba"
        self.bucket_name = "bucket_name"
        # Se apunta a las carpetas de los flows
        self.storage_client = storage.Client.from_service_account_json(
            'service_account.json')
        self.bigquery_client = bigquery.Client.from_service_account_json(
            'service_account.json')
        credentials = service_account.Credentials.from_service_account_file(
            'service_account.json')
        credentials.with_scopes(
            ['https://www.googleapis.com/auth/spreadsheets'])
        self.googlesheet_client = build(
            'sheets', 'v4', credentials=credentials)

    def uploadBigQuery(self, df: DataFrame, dataset=None, table=None, action=None, sp=None,
                       partition_field: str = None, clustering_fields: list = None):
        """Upload a DataFrame to table in BigQuery.

        Args:
            df: DataFrame to upload.
            dataset: Name of dataset in GCP BigQuery.
            table: Name of table in GCP BigQuery.
            action: (append or replace) Action to apply to tabla and data.
            sp: (Optional) Name of store procedure that will execute after of upload.

        Returns:
            None.
        """

        if not df.empty:
            write_mode = "WRITE_APPEND" if action == "append" else "WRITE_TRUNCATE"
            schema_table = self.getSchema(df=df)
            dir_table = f"{self.project_id}.{dataset}.{table}"
        
            # --- Configuración segura de carga ---
            job_config = bigquery.LoadJobConfig(
                schema=schema_table,
                write_disposition=write_mode
            )
        
            # Añadir partición solo si se especifica
            if partition_field:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.HOUR,
                    field=partition_field
                )
        
            # Añadir clustering solo si se especifica y tiene columnas
            if clustering_fields and len(clustering_fields) > 0:
                job_config.clustering_fields = clustering_fields
        
            job = self.bigquery_client.load_table_from_dataframe(
                df, dir_table, job_config=job_config
            )
            job.result()

        if sp:
            QUERY = (f'CALL `{sp}`();')
            query_job = self.bigquery_client.query(QUERY)  # API request
            rows = query_job.result()
            print("Se ejecuto procedure")

    def callProcedure(self, sp: str):
        QUERY = (f'CALL `{sp}`();')
        query_job = self.bigquery_client.query(QUERY)
        rows = query_job.result()
        print(f"Se ejecuto procedure")        

    def getSchema(self, df: DataFrame) -> list:

        schema = []
        dtypes = df.dtypes.to_dict()
        for col, col_type in dtypes.items():

            if col_type.name == "datetime64[ns]":
                schema.append(bigquery.SchemaField(
                    col, bigquery.enums.SqlTypeNames.DATETIME))

            elif col_type.name == "float64":
                schema.append(bigquery.SchemaField(
                    col, bigquery.enums.SqlTypeNames.FLOAT64))

            else:
                schema.append(bigquery.SchemaField(
                    col, bigquery.enums.SqlTypeNames.STRING))

        return schema



