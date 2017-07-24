import os
import errno
import threading
import time
import sys

from osgeo import gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum
from subprocess import call

# Imports the Google Cloud client library
from google.cloud import bigquery

# Failing Jupyter code
# import datetime
#
# from lxml import etree
# from osgeo import gdal
# from urllib.parse import urlparse
# from datetime import date
# from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata
# metadataService = MetadataService()
# d_end = date(2016, 6, 24)
# d_start = date(2015, 6, 24)
# bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
# rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
#                        limit=1)
# base_mount_path = '/imagery'
# metadata = Metadata(rows[0], base_mount_path)
# gsurl = urlparse(metadata.base_url)
# storage = Storage(gsurl[1])
#
# b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
# landsat = Landsat(base_mount_path, gsurl[2])
# vrt = landsat.get_vrt(metadata, [5,4,3])
#
# dataset = gdal.Open(vrt)
# nda=dataset.ReadAsArray().transpose((1, 2, 0))
# nda.shape
# import matplotlib.pyplot as plt
# %matplotlib inline
# plt.figure(figsize=[16,16])
# plt.imshow(nda)

class SpacecraftID(Enum):
    LANDSAT_8 = 8
    LANDSAT_7 = 7
    LANDSAT_5 = 5
    LANDSAT_4 = 4
    LANDSAT_1 = 1
    LANDSAT_3 = 3
    LANDSAT_2 = 2


class Landsat:
    bucket_name = ""
    base_mount_path = ""
    storage = None

    def __init__(self, base_mount_path, bucket_name=None):
        self.bucket_name = "gcp-public-data-landsat"
        self.base_mount_path = base_mount_path
        self.storage = Storage(self.bucket_name)

    def fetch_imagery_array(self, bucket_sub_folder, band_numbers):

        # TODO
        if self.storage.mount_sub_folder(bucket_sub_folder, self.base_mount_path) is False:
            return None

    @staticmethod
    def get_raster_band_elem(
            vrt_dataset,
            data_type,
            position_number,
            file_path,
            x_size,
            y_size,
            block_size,
            color_interp=None):

        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        if color_interp is not None:
            etree.SubElement(elem_raster_band, "ColorInterp").text = color_interp

        elem_raster_band.set("dataType", data_type)
        elem_raster_band.set("band", str(position_number))

        elem_simple_source = etree.SubElement(elem_raster_band, "SimpleSource")

        elem_source_filename = etree.SubElement(elem_simple_source, "SourceFilename")
        elem_source_filename.set("relativeToVRT", "0")
        elem_source_filename.text = file_path

        elem_source_props = etree.SubElement(elem_simple_source, "SourceProperties")
        elem_source_props.set("RasterXSize", str(x_size))
        elem_source_props.set("RasterYSize", str(y_size))
        elem_source_props.set("DataType", data_type)

        # there may be a more efficient size than 256
        elem_source_props.set("BlockXSize", str(block_size))
        elem_source_props.set("BlockYSize", str(block_size))

        # if the input had multiple bands this setting would be where you change that
        # but the google landsat is one tif per band
        etree.SubElement(elem_simple_source, "SourceBand").text = str(1)

        elem_src_rect = etree.SubElement(elem_simple_source, "SrcRect")
        elem_src_rect.set("xOff", str(0))
        elem_src_rect.set("yOff", str(0))
        elem_src_rect.set("xSize", str(x_size))
        elem_src_rect.set("ySize", str(y_size))

        elem_dst_rect = etree.SubElement(elem_simple_source, "DstRect")
        elem_dst_rect.set("xOff", str(0))
        elem_dst_rect.set("yOff", str(0))
        elem_dst_rect.set("xSize", str(x_size))
        elem_dst_rect.set("ySize", str(y_size))

    def get_vrt(self, metadata, band_numbers):
        vrt_dataset = etree.Element("VRTDataset")

        position_number = 1
        max_x = sys.float_info.min
        max_y = sys.float_info.min

        for band in band_numbers:
            file_path = metadata.full_mount_path + os.path.sep + metadata.scene_id + "_B{}.TIF".format(band)
            dataset = gdal.Open(file_path)
            # TODO, check that this matters. I think maybe it doesn't
            max_x = dataset.RasterXSize if dataset.RasterXSize > max_x else max_x
            max_y = dataset.RasterYSize if dataset.RasterYSize > max_y else max_y

            color_interp = None
            if band == 4:
                color_interp = "Red"
            elif band == 3:
                color_interp = "Green"
            elif band == 2:
                color_interp = "Blue"

            self.get_raster_band_elem(
                vrt_dataset,
                "UInt16",
                position_number,
                file_path,
                dataset.RasterXSize,
                dataset.RasterYSize,
                256,
                color_interp=color_interp)

            position_number += 1

        vrt_dataset.set("rasterXSize", str(max_x))
        vrt_dataset.set("rasterYSize", str(max_y))
        etree.SubElement(vrt_dataset, "SRS").text = dataset.GetProjection()
        etree.SubElement(vrt_dataset, "GeoTransform").text = ",".join(map("  {:.16e}".format, dataset.GetGeoTransform()))

        return etree.tostring(vrt_dataset)

    def get_ndarray(self, scales, band_numbers, metadata):
        vrt = self.get_vrt(metadata, band_numbers)
        # http://gdal.org/python/
        # http://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
        translateOptions = gdal.TranslateOptions(scaleParams=[[0, 4000], [0, 4000], [0, 4000]])
        vrt_projected = gdal.Translate('', vrt, translateOptions)
        dataset = gdal.Open(vrt_projected)
        nda = dataset.ReadAsArray().transpose((1, 2, 0))
        return nda




class Sentinel2:
    bucket_name = ""


class Metadata:
    def __init__(self, row, base_mount_path=None):
        self.scene_id = row[0]  # STRING	REQUIRED   Unique identifier for a particular Landsat image downlinked to a particular ground station.
        self.product_id = row[1]  # STRING	NULLABLE Unique identifier for a particular scene processed by the USGS at a particular time, or null for pre-collection data.
        self.spacecraft_id = row[2]  # STRING	NULLABLE The spacecraft that acquired this scene: one of 'LANDSAT_4' through 'LANDSAT_8'.
        self.sensor_id = row[3]  # STRING	NULLABLE The type of spacecraft sensor that acquired this scene: 'TM' for the Thematic Mapper, 'ETM' for the Enhanced Thematic Mapper+, or 'OLI/TIRS' for the Operational Land Imager and Thermal Infrared Sensor.
        self.date_acquired = row[4]  # STRING	NULLABLE The date on which this scene was acquired (UTC).
        self.sensing_time = row[5]  # STRING	NULLABLE The approximate time at which this scene was acquired (UTC).
        self.collection_number = row[6]  # STRING	NULLABLE The Landsat collection that this image belongs to, e.g. '01' for Collection 1 or 'PRE' for pre-collection data.
        self.collection_category = row[7]  # STRING	NULLABLE Indicates the processing level of the image: 'RT' for real-time, 'T1' for Tier 1, 'T2' for Tier 2, and 'N/A' for pre-collection data. RT images will be replaced with Tier 1 or Tier 2 images as they become available.
        self.data_type = row[8]  # STRING	NULLABLE The type of processed image, e.g. 'L1T' for Level 1 terrain-corrected images.
        self.wrs_path = row[9]  # INTEGER	NULLABLE The path number of this scene's location in the Worldwide Reference System (WRS).
        self.wrs_row = row[10]  # INTEGER	NULLABLE The row number of this scene's location in the Worldwide Reference System (WRS).
        self.cloud_cover = row[11]  # FLOAT	NULLABLE Estimated percentage of this scene affected by cloud cover.
        self.north_lat = row[12]  # FLOAT	NULLABLE The northern latitude of the bounding box of this scene.
        self.south_lat = row[13]  # FLOAT	NULLABLE The southern latitude of the bounding box of this scene.
        self.west_lon = row[14]  # FLOAT	NULLABLE The western longitude of the bounding box of this scene.
        self.east_lon = row[15]  # FLOAT	NULLABLE The eastern longitude of the bounding box of this scene.
        self.total_size = row[16]  # INTEGER	NULLABLE The total size of this scene in bytes.
        self.base_url = row[17]  # STRING	NULLABLE The base URL for this scene in Cloud Storage.
        gsurl = urlparse(self.base_url)
        self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + gsurl[2].strip("\/")


class MetadataService:
    def __init__(self):
        self.m_client = bigquery.Client()
        self.m_timeout_ms = 10000

        # these values were define on July 16, 2017, buffered by half a degree
        self.m_danger_east_lon = -165.0661 + 0.5
        self.m_danger_west_lon = 165.59402 - 0.5

        # TODO, this is overkill. probably, best to use continuous integration to check once a day and update danger
        # TODO zone as needed. and for now, just buffer the danger zone by a half a degree
        # do some async query to check if the danger_zone needs updating
        # thread = threading.Thread(target=self.__get_danger_zone, args=())
        # thread.daemon = True  # Daemonize thread
        # thread.start()

    # TODO this is probably overkill
    def __get_danger_zone(self):
        west_lon_sql = """SELECT west_lon
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] 
WHERE west_lon = (
SELECT MIN(west_lon)
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index]
WHERE east_lon < 0
AND west_lon > 0 )
LIMIT 1"""

        east_lon_sql = """SELECT east_lon
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] 
WHERE east_lon = (
SELECT MAX(east_lon)
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index]
WHERE east_lon < 0
AND west_lon > 0 )
LIMIT 1"""
        query = self.m_client.run_sync_query(west_lon_sql)
        query.timeout_ms = self.m_timeout_ms
        query.run()
        self.m_danger_west_lon = query.rows[0][0]
        query = self.m_client.run_sync_query(east_lon_sql)
        query.timeout_ms = self.m_timeout_ms
        query.run()
        self.m_danger_east_lon = query.rows[0][0]

    def search(self, satellite_id, bounding_box=None, start_date=None, end_date=None, sort_by=None, limit=10, sql_filters=None):
        # # Perform a synchronous query.
        query_builder = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] ' \
                        'WHERE spacecraft_id="{}"'.format(satellite_id.name)

        if bounding_box is not None:
            minx = bounding_box[0]
            miny = bounding_box[1]
            maxx = bounding_box[2]
            maxy = bounding_box[3]

            # dateline danger zone
            # TODO, this needs to be refined. might be catching too many cases.
            if minx > maxx \
                    or maxx > self.m_danger_west_lon \
                    or minx > self.m_danger_west_lon \
                    or maxx < self.m_danger_east_lon \
                    or minx < self.m_danger_east_lon:
                print("danger zone. you're probably in trouble")
            else:
                query_builder += ' AND (({0} <= west_lon AND {1} >= west_lon) OR ' \
                                 '({0} >= west_lon AND east_lon >= {0}))'.format(minx, maxx)
                query_builder += ' AND ((south_lat <= {0} AND north_lat >= {0}) OR ' \
                                 '(south_lat > {0} AND {1} >= south_lat))'.format(miny, maxy)

        if start_date is not None:
            query_builder += ' AND date_acquired>="{}"'.format(start_date.isoformat())
        if end_date is not None:
            query_builder += ' AND date_acquired<="{}"'.format(end_date.isoformat())

        if sql_filters is not None:
            for sql_filter in sql_filters:
                print(sql_filter)

        # TODO sort by area
        """
lifted from esri-geometry-api
if (isEmpty() || other.isEmpty())
    return false;

if (other.xmin > xmin)
    xmin = other.xmin;

if (other.xmax < xmax)
    xmax = other.xmax;

if (other.ymin > ymin)
    ymin = other.ymin;

if (other.ymax < ymax)
    ymax = other.ymax;

boolean bIntersecting = xmin <= xmax && ymin <= ymax;

if (!bIntersecting)
    setEmpty();

return bIntersecting;"""

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
        self.base_path = None

    def mount_sub_folder(self, bucket_sub_folder, base_path):
        # execute mount command
        # gcsfuse --only-dir LC08/PRE/044/034/LC80440342016259LGN00 gcp-public-data-landsat /landsat

        full_mount_path = base_path.rstrip("\/") + os.path.sep + bucket_sub_folder.strip("\/")
        # subprocess.run("exit 1", shell=True, check=True)
        # subprocess.run(["ls", "-l", "/dev/null"], stdout=subprocess.PIPE)
        if full_mount_path in self.mounted_sub_folders:
            return True

        try:
            if not os.path.isdir(full_mount_path):
                os.makedirs(full_mount_path)
            else:
                # check to see if directory is already mounted if so maybe just return True?
                if len(os.listdir(full_mount_path)) > 0:
                    return True
                # hard to know what to do if it's mounted and it's empty...
                # TODO make a test for that case

        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        val = call(["gcsfuse", "--only-dir", bucket_sub_folder.lstrip("\/"), self.bucket, full_mount_path])
        # TODO return error message if necessary
        if val != 0:
            return False

        self.mounted_sub_folders[full_mount_path] = True
        return True



