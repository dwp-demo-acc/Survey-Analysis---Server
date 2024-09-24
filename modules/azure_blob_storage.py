import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from typing import Callable, Any, Literal
from dotenv import load_dotenv

load_dotenv()



class AzureBlobStorage:
    _instance = None
    _container_name = 'data'

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _get_file_read_mode(self, file_name: str, read_or_write: Literal['w', 'r']):
        read_text_format = ['csv', ]  # todo: add more
        read_binary_format = ['txt', 'xls', 'xlsx', 'jpg', 'jpeg', 'png', "json"]  # todo: complete more

        for suffix in read_text_format:
            if file_name.endswith(suffix):
                return read_or_write + 't'  # rt or wt

        for suffix in read_binary_format:
            if file_name.endswith(suffix):
                return read_or_write + 'b'  # rb or wb

        return read_or_write

    def _initialize_connection(self):
        connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # upload context file must added the context_<fileName>
    # upload survey file must added the survey_<fileName>
    def upload_blob(self, file, file_name, updated_name=None):
        if updated_name is None:
            updated_name = file_name

        blob_client = self.blob_service_client.get_blob_client(container=self._container_name, blob=updated_name)
        blob_client.upload_blob(file)
        return 'success'

    def get_list_blobs(self):
        container_client = self.blob_service_client.get_container_client(container=self._container_name)
        return container_client.list_blobs()

    def download_blob(self, blob_name: str, download_directory: str, cb: Callable[[bytes], Any]):

        download_file_path = os.path.join(download_directory, blob_name)

        container_client = self.blob_service_client.get_container_client(container=self._container_name)
        with open(file=download_file_path, mode=self._get_file_read_mode(blob_name, 'w')) as download_file:
            download_file.write(container_client.download_blob(blob_name).readall())
            return cb(download_file)

    def read_blob(self, blob_name: str) -> bytes:
        try:
            container_client = self.blob_service_client.get_container_client(container=self._container_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_content = blob_client.download_blob().readall()
            return blob_content
        except ResourceNotFoundError as e:
            custom_error = f"Blob '{blob_name}' not found in container '{self._container_name}'."
            print(custom_error)
            raise FileNotFoundError(custom_error) from e
        except Exception as e:
            print(e)
            raise Exception("Error reading blob.") from e

