from enum import Enum
from subprocess import call

import os
import errno


class SatelliteID(Enum):
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

    def search(self, satellite_id, bounding_box=None, start_date=None, end_date=None, limit=10):

        return []


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
