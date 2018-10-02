from threading import Lock
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time
import datetime
import settings
import initialization_exception as ie


class FactoryManager:

    @staticmethod
    def print_activity_run_details(activity_run):
        """Print activity run details."""
        print("\n\tActivity run details\n")
        print("\tActivity run status: {}".format(activity_run.status))
        if activity_run.status == 'Succeeded':
            print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
            print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
            print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
        else:
            print("\tErrors: {}".format(activity_run.error['message']))

    # Stuff for async uploading that never got set up.
    lock = Lock()
    instance_number = 0
    blob_ls_name = 'blob_linked_storage_'
    db_ls_name = 'db_linked_storage_'

    csv_format = TextFormat(first_row_as_header=settings.first_row_as_header,
                            column_delimiter=settings.column_delimiter,
                            skip_line_count=settings.skip_line_count,
                            quote_char=settings.quote_character,
                            escape_char=settings.escape_character,
                            null_value=settings.null_value,
                            encoding_name=settings.encoding_name)

    def __init__(self):
        FactoryManager.lock.acquire()
        self.instance_number = FactoryManager.instance_number
        FactoryManager.instance_number += 1
        FactoryManager.lock.release()

        self.blob_ls_instance_name = FactoryManager.blob_ls_name + str(self.instance_number)
        self.db_ls_instance_name = FactoryManager.db_ls_name + str(self.instance_number)
        credentials = ServicePrincipalCredentials(client_id=settings.AD_client_id, secret=settings.AD_client_secret, tenant=settings.AD_tenant_id)

        #  Get the resource group specified in settings
        print('Getting resource group ' + settings.rg_name)
        resource_client = ResourceManagementClient(credentials, settings.subscription_id)
        resource_client.resource_groups.create_or_update(settings.rg_name, settings.rg_params)

        #  Get the data factory specified in settings, or create it if it does not exist.
        self.adf_client = DataFactoryManagementClient(credentials, settings.subscription_id)
        df_resource = Factory(settings.df_params)
        try:
            df = self.adf_client.factories.create_or_update(settings.rg_name, settings.df_name, df_resource)
            print('Created factory ' + settings.df_name)
        except ErrorResponseException:
            print('Factory '+settings.df_name+' already exists.  Skipping factory creation')
            try:
                df = self.adf_client.factories.get(settings.rg_name, settings.df_name)
            except ErrorResponseException:
                print('Factory '+settings.df_name+' already exists, but is not a member of resource group '+settings.rg_name)
                raise ie.InitializationException

        while df.provisioning_state != 'Succeeded':
            df = self.adf_client.factories.get(settings.rg_name, settings.df_name)
            time.sleep(1)

        # Get blob linked storage
        blob_azure_storage = AzureStorageLinkedService(connection_string=
                                                        SecureString(
                                                         'DefaultEndpointsProtocol=https;'
                                                         'AccountName='+settings.blob_account_name +
                                                         ';AccountKey='+settings.blob_account_key
                                                        )
                                                    )
        self.adf_client.linked_services.create_or_update(settings.rg_name, settings.df_name, self.blob_ls_instance_name, blob_azure_storage)

        # Get azure linked storage

        secure_string = SecureString(
            'Server=tcp:'+settings.server+',1433'
            ';Database=' + settings.database +
            ';User ID='+settings.username+'@'+settings.server_name +
            ';Password='+settings.password +
            ';Trusted_Connection=False'
            ';Encrypt=True'
            ';Connection Timeout=30'
        )

        if settings.warehouse:
            db_azure_storage = AzureSqlDWLinkedService(connection_string=secure_string)
        else:
            db_azure_storage = AzureSqlDatabaseLinkedService(connection_string=secure_string)

        self.adf_client.linked_services.create_or_update(settings.rg_name, settings.df_name, self.db_ls_instance_name, db_azure_storage)

    def copy(self, blob_name, table_name):
        print("Copying "+blob_name+" to "+table_name)

        # Get the source dataset from the blob
        source_name = 'db_transfer_in_'+str(self.instance_number)
        ds_in_ls = LinkedServiceReference(self.blob_ls_instance_name)

        ds_in = AzureBlobDataset(ds_in_ls, folder_path=settings.container_name, file_name=blob_name, format=FactoryManager.csv_format)
        self.adf_client.datasets.create_or_update(settings.rg_name, settings.df_name, source_name, ds_in)

        # Get the sink dataset from the SQL table
        sink_name = 'db_transfer_sink_'+str(self.instance_number)
        ds_sink_ls = LinkedServiceReference(self.db_ls_instance_name)
        ds_sink = AzureSqlDWTableDataset(ds_sink_ls, table_name) if settings.warehouse else AzureSqlTableDataset(ds_sink_ls, table_name)
        self.adf_client.datasets.create_or_update(settings.rg_name, settings.df_name, sink_name, ds_sink)

        # Get the action from the two datasets
        act_name = 'copy_CSV_to_SQL'
        blob_source = BlobSource()
        sql_sink = SqlDWSink() if settings.warehouse else SqlSink()
        dsin_ref = DatasetReference(source_name)
        dsOut_ref = DatasetReference(sink_name)
        copy_activity = CopyActivity(act_name, inputs=[dsin_ref], outputs=[dsOut_ref], source=blob_source, sink=sql_sink)

        # Get the pipeline that performs the action
        p_name = 'CSV_to_SQL_pipeline'
        params_for_pipeline = {}
        p_obj = PipelineResource(activities=[copy_activity], parameters=params_for_pipeline)
        self.adf_client.pipelines.create_or_update(settings.rg_name, settings.df_name, p_name, p_obj)

        # Run the pipeline
        run_response = self.adf_client.pipelines.create_run(settings.rg_name, settings.df_name, p_name, {})

        pipeline_run = self.adf_client.pipeline_runs.get(settings.rg_name, settings.df_name, run_response.run_id)
        while pipeline_run.status[0] == 'I':
            time.sleep(1)
            pipeline_run = self.adf_client.pipeline_runs.get(settings.rg_name, settings.df_name, run_response.run_id)

        if pipeline_run.status[0] == 'S':
            return True

        print("\n\tPipeline run status: {}".format(pipeline_run.status))
        activity_runs_paged = list(self.adf_client.activity_runs.list_by_pipeline_run(settings.rg_name, settings.df_name, pipeline_run.run_id,
                                                                                 datetime.datetime.now() - datetime.timedelta(1),
                                                                                 datetime.datetime.now() + datetime.timedelta(1)))
        FactoryManager.print_activity_run_details(activity_runs_paged[0])
        return False


