import os
import errno

from enum import Enum
from subprocess import call
# Imports the Google Cloud client library
from google.cloud import bigquery

class SpacecraftID(Enum):
    LANDSAT_8 = 1
    LANDSAT_7 = 2


class Landsat:
    bucket_name = ""
    base_mount_path = ""
    storage = None

    def __init__(self, base_mount_path):
        self.bucket_name = "gcp-public-data-landsat"
        self.base_mount_path = base_mount_path
        self.storage = Storage(self.bucket_name)

    def fetch_imagery(self, file_id):

        if self.storage.mount_sub_folder("", "") is False:
            return False


class Sentinel2:
    bucket_name = ""


class Metadata:
    def __init__(self):
        self.m_client = bigquery.Client()
        self.m_timeout_ms = 10000

    def search(self, satellite_id, bounding_box=None, start_date=None, end_date=None, sort_by=None, limit=10, sql_filters=None):
        # # Perform a synchronous query.
        query_builder = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] ' \
                        'WHERE spacecraft_id="{}"'.format(satellite_id.name)

        if bounding_box is not None:
            # north_lat	south_lat	west_lon	east_lon
            query_builder += ' AND '

            query_builder += ' AND bounding_box={}'.format(bounding_box)

        if start_date is not None:
            query_builder += ' AND date_acquired>="{}"'.format(start_date.isoformat())

        if end_date is not None:
            query_builder += ' AND date_acquired<="{}"'.format(end_date.isoformat())

        if sql_filters is not None:
            for sql_filter in sql_filters:
                print(sql_filter)

        if sort_by is not None:
            query_builder += ' SORT BY {}'.format(sort_by)

        query = self.m_client.run_sync_query('{} LIMIT {}'.format(query_builder, limit))
        query.timeout_ms = self.m_timeout_ms
        query.run()

        return query.rows


class Storage:
    bucket = ""
    mounted_sub_folders = None

    def __init__(self, bucket_name):
        self.bucket = bucket_name
        self.mounted_sub_folders = {}

    def mount_sub_folder(self, bucket_sub_folder, full_mount_path):
        # execute mount command
        # gcsfuse --only-dir LC08/PRE/044/034/LC80440342016259LGN00 gcp-public-data-landsat /landsat

        # subprocess.run("exit 1", shell=True, check=True)
        # subprocess.run(["ls", "-l", "/dev/null"], stdout=subprocess.PIPE)
        if full_mount_path in self.mounted_sub_folders:
            return True

        try:
            os.makedirs(full_mount_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        val = call(["gcsfuse", "--only-dir", bucket_sub_folder, self.bucket, full_mount_path])
        if val != 1:
            return False

        self.mounted_sub_folders[full_mount_path] = True
        return True
