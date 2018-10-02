from azure.common import AzureMissingResourceHttpError
import settings
import io
from azure.storage.blob import BlockBlobService

block_blob_service = None
generator = None
generator_container_name = None
uploaded_container_name = None


def initialize():
    global block_blob_service
    if block_blob_service is None:
        block_blob_service = BlockBlobService(account_name=settings.blob_account_name, account_key=settings.blob_account_key)


def has_blob(container_name, blob_name):
    global generator, generator_container_name

    if generator_container_name != container_name:
        generator = block_blob_service.list_blobs(container_name)
        generator_container_name = container_name

    for blob in generator:
        if blob.name == blob_name:
            return True

    return False


def upload_blob(container_name, file_path, blob_name):
    global uploaded_container_name
    if uploaded_container_name != container_name:
        try:
            block_blob_service.get_container_properties(container_name)
        except AzureMissingResourceHttpError:
            print("Creating container \""+container_name+"\" in " + settings.blob_account_name)
            if not block_blob_service.create_container(container_name):
                print("Could not create container \"" + container_name + "\" in " + settings.blob_account_name)
                return False

    uploaded_container_name = container_name

    # Upload an empty file of the same name to commit any uncommited blocks of the same name.
    block_blob_service.create_blob_from_bytes(container_name, blob_name, bytes())

    print('Uploading '+file_path+' to '+container_name+' as '+blob_name)
    block_blob_service.create_blob_from_path(container_name, blob_name, file_path)
