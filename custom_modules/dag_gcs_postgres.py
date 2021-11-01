import json
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io



class GoogleCloudStorageToPostgresTransfer(BaseOperator):

    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table')

    template_ext = ('.sql',)

    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 schema,
                 key,
                 source_objects,
                 destination_project_dataset_table,
                 verify=None,
                 schema_fields=None,
                 schema_object=None,
                 wildcard_match=False,
                 source_format='CSV',
                 compression='NONE',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 ignore_unknown_values=False,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 encoding="UTF-8",
                 max_id_key=None,
                 gcs_conn_postgres_id='postgres_default',
                 google_cloud_storage_conn_id ='gcp_default',
                 delegate_to=None,
                 schema_update_options=(),
                 src_fmt_configs=None,
                 external_table=False,
                 time_partitioning=None,
                 cluster_fields=None,
                 autodetect=True,
                 encryption_configuration=None,
                 *args, **kwargs):

        super(GoogleCloudStorageToPostgresTransfer, self).__init__(*args, **kwargs)

        # GCS config
        if src_fmt_configs is None:
            src_fmt_configs = {}
        if time_partitioning is None:
            time_partitioning = {}
        self.bucket = bucket
        self.key = key
        self.source_objects = source_objects
        self.schema = schema
        self.schema_object = schema_object
        self.verify = verify
        self.wildcard_match = wildcard_match

        # popstgres config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table
        self.encoding = encoding

        self.max_id_key = max_id_key
        self.postgres_conn_id = postgres_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.autodetect = autodetect
        self.encryption_configuration = encryption_configuration

    def execute(self, context):
        
        # Create an instances to connect gcs and Postgres DB.
        self.log.info(self.gcs_conn_postgres_id)   
        
        self.pg_hook = PostgresHook(postgre_conn_id = self.gcs_conn_postgres_id)
        self.gcs = GoogleCloudStorageHook(google_cloud_storage_conn_id = self.google_cloud_storage_conn_id, verify = self.verify)

        self.log.info("Downloading gcs file")
        self.log.info(self.key + ', ' + self.bucket)

        # Validate if the file source exist or not in the bucket.
        if self.wildcard_match:
            if not self.gcs.check_for_wildcard_key(self.key, self.bucket):
                raise AirflowException("No key matches {0}".format(self.key))
            key_object = self.gcs.get_wildcard_key(self.key, self.bucket)
        else:
            if not self.gcs.check_for_key(self.key, self.bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.key))
                  
            key_object = self.gcs.get_key(self.key, self.bucket)

         # Read and decode the file into a list of strings.  
        list_srt_content = key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
        
        # schema definition for data types of the source.
        schema = {
                    'invoice_number': 'string',
                    'stock_code': 'string',
                    'detail': 'string',
                    'quantity': 'float64',
                    'unit_price': 'float64',                                
                    'customer_id': 'string',
                    'country': 'string',
                 }  
        date_cols = ['invoice_date']      

        # read a csv file with the properties required.
        user_purchase = pd.read_csv(io.StringIO(list_srt_content), 
                         header=0, 
                         delimiter=",",
                         quotechar='"',
                         low_memory=False,
                         #parse_dates=date_cols,                                             
                         dtype=schema                         
                         )
        self.log.info(user_purchase)
        self.log.info(user_purchase.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        user_purchase = user_purchase.replace(r"[\"]", r"'")
        list_user_purchase = user_purchase.values.tolist()
        list_user_purchase = [tuple(x) for x in list_user_purchase]
        self.log.info(list_user_purchase)   
       
        # Read the file with the DDL SQL to create the table products in postgres DB.
        nombre_de_archivo = "bootcampdb.products.sql"
        
        ruta_archivo = '/usr/local/airflow/custom_modules/assets' + os.path.sep + nombre_de_archivo
        self.log.info(ruta_archivo)
        proposito_del_archivo = "r" #r es de Lectura
        codificación = "UTF-8" #Tabla de Caracteres,
                               #ISO-8859-1 codificación preferidad por
                               #Microsoft, en Linux es UTF-8

        with open(ruta_archivo, proposito_del_archivo, encoding=codificación) as manipulador_de_archivo:
       
            #Read dile with the DDL CREATE TABLE
            SQL_COMMAND_CREATE_TBL = manipulador_de_archivo.read()
            manipulador_de_archivo.close()

            #Display the content 
            self.log.info(SQL_COMMAND_CREATE_TBL)    

        # execute command to create table in postgres.  
        self.pg_hook.run(SQL_COMMAND_CREATE_TBL)  
        
        # set the columns to insert, in this case we ignore the id, because is autogenerate.
        list_target_fields = ['invoice_number', 
                              'stock_code',
                              'detail', 
                              'quantity',
                              'invoice_date' 
                              'unit_price', 
                              'customer_id', 
                              'country']
        
        self.current_table = self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,  
                                 list_user_purchase, 
                                 target_fields = list_target_fields, 
                                 commit_every = 1000,
                                 replace = False)
        
         # Query and print the values of the table products in the console.
        self.request = 'SELECT * FROM ' + self.current_table
        self.log.info(self.request)
        self.connection = self.pg_hook.get_conn()
        self.cursor = self.connection.cursor()
        self.cursor.execute(self.request)
        self.sources = self.cursor.fetchall()
        self.log.info(self.sources)

        for source in self.sources:           
            self.log.info("invoice_number: {0} - \
                           stock_code: {1} - \
                           detail: {2} - \
                           quantity: {3} - \
                           invoice_date: {4} - \
                           unit_price: {5} - \
                           customer_id: {6} - \
                           country: {7} ".format(source[0],source[1],source[2],source[3],source[4],source[5], 
                                                   source[6],
                                                   source[7]))

   
