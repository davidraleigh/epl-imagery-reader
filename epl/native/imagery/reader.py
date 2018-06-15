import os
import sys
import errno
import tempfile
import py_compile

import shapefile

# TODO replace with geometry
import shapely.wkb
import shapely.wkt
from shapely.geometry import shape
# TODO replace with geometry

import json
import math
import pyproj
import copy
import glob
import re
import numpy as np

from pyqtree import Index

from typing import Generator
from operator import itemgetter
from datetime import date
from datetime import datetime
from osgeo import osr, ogr, gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum
from subprocess import call

from typing import List, Tuple
from peewee import Field
# Imports the Google Cloud client library
from google.cloud import bigquery, storage
from google.cloud import exceptions

from epl.grpc.imagery import epl_imagery_pb2
from epl.native.imagery import PLATFORM_PROVIDER
from epl.native.imagery.metadata_helpers import SpacecraftID, Band, BandMap, MetadataFilters, LandsatQueryFilters


class __Singleton(type):
    """
    https://sourcemaking.com/design_patterns/singleton/python/1
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class DataType(Enum):
    # Byte, UInt16, Int16, UInt32, Int32, Float32, Float64, CInt16, CInt32, CFloat32 or CFloat64
    """enum DataType {
    UKNOWN = 0;
    BYTE = 0;  1
    INT16 = 1; 2
    UINT16 = 2; 4

    INT32 = 3; 8
    UINT32 = 4; 16
    FLOAT32 = 5; 32
    FLOAT64 = 6; 64
    CFLOAT32 = 7; 128
    CFLOAT64 = 8; 256
}
http://www.gdal.org/gdal_8h.html#a22e22ce0a55036a96f652765793fb7a4
https://docs.scipy.org/doc/numpy-1.13.0/reference/arrays.dtypes.html#arrays-dtypes
https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    """
    UNKNOWN_GDAL = (gdal.GDT_Unknown,   "Unknown",  1,           -1,         0, np.void)

    BYTE         = (gdal.GDT_Byte,      "Byte",     0,           255,        1, np.uint8)

    UINT16       = (gdal.GDT_UInt16,    "UInt16",   0,           65535,      2, np.uint16)
    INT16        = (gdal.GDT_Int16,     "Int16",    -32768,      32767,      3, np.int16)

    UINT32       = (gdal.GDT_UInt32,    "UInt32",   0,           4294967295, 4, np.uint32)
    INT32        = (gdal.GDT_Int32,     "Int32",    -2147483648, 2147483647, 5, np.int32)

    FLOAT32      = (gdal.GDT_Float32,   "Float32",  -3.4E+38,    3.4E+38,    6, np.float32)
    FLOAT64      = (gdal.GDT_Float64,   "Float64",  -1.7E+308,   1.7E+308,   7, np.float64)

    # CINT16
    # CINT32
    #TODO I think these ranges are reversed CFloat32 and CFloat64
    CFLOAT32     = (gdal.GDT_CFloat32, "CFloat32", -1.7E+308,   1.7E+308,   10, np.complex64)
    CFLOAT64     = (gdal.GDT_CFloat64, "CFloat64", -3.4E+38,    3.4E+38,    11, np.complex64)

    def __init__(self, gdal_type, name, range_min, range_max, grpc_num, numpy_type):
        self.__gdal_type = gdal_type
        self.__name = name
        self.range_min = range_min
        self.range_max = range_max
        self.__grpc_num = grpc_num
        self.__numpy_type = numpy_type

    def __or__(self, other):
        return self.__grpc_num | other.__grpc_num

    def __and__(self, other):
        return self.__grpc_num & other.__grpc_num

    @property
    def gdal(self):
        return self.__gdal_type

    @property
    def name(self):
        return self.__name

    @property
    def grpc_num(self):
        return self.__grpc_num

    @property
    def numpy_type(self):
        return self.__numpy_type


class FileTypeMap:
    @staticmethod
    def get_suffix(file_type):
        return ".jpg"


class FunctionDetails:
    """
    Make a pixel function
    """
    name = None
    band_definitions = None
    data_type = None
    code = None
    arguments = None
    transfer_type = None

    def __init__(self,
                 name: str,
                 band_definitions: list,
                 data_type: DataType,
                 code: str=None,
                 arguments: dict=None,
                 transfer_type: DataType=None):
        self.name = name
        self.band_definitions = band_definitions
        self.data_type = data_type

        if code:
            # TODO, still ugly that I have to use a temporary file: Also, stupid that I can't catch GDAL errors
            function_file = tempfile.NamedTemporaryFile(prefix=self.name, suffix=".py", delete=True)
            function_file.write(code.encode())
            function_file.flush()

            py_compile.compile(function_file.name, doraise=True)
            # delete file after compiling
            function_file.close()
            self.code = code

        # TODO arguments should maybe have some kind of setter
        if arguments:
            self.arguments = {k: str(v) for k, v in arguments.items()}

        self.transfer_type = transfer_type


# TODO rename as LandsatMetadata
class Metadata:
    __storage_client = storage.Client()
    metadata_reg = re.compile(r'/imagery/c1/L8/([\d]{3,3})/([\d]{3,3})/L(C|T)08_([a-zA-Z0-9]+)_[\d]+_([\d]{4,4})([\d]{2,2})([\d]{2,2})_[\d]+_[a-zA-Z0-9]+_(RT|T1|T2)')
    datetime_reg = re.compile(r'([\w\-:]+)(\.[\d]{0,6})[\d]*([A-Z]{1})')

    """
    LXSS_LLLL_PPPRRR_YYYYMMDD_yyyymmdd_CC_TX_BN.TIF where:
     L           = Landsat
     X           = Sensor (E for ETM+ data; T for TM data; M for MSS)
     SS          = Satellite (07 = Landsat 7, 05 = Landsat 5, etc.)
     LLLL        = processing level (L1TP for Precision Terrain;
                                     L1GT for Systematic Terrain;
                                     L1GS for Systematic only)
     PPP         = starting path of the product
     RRR         = starting and ending rows of the product
     YYYY        = acquisition year
     MM          = acquisition month
     DD          = acquisition day
     yyyy        = processing year
     mm          = processing month
     dd          = processing day
     CC          = collection number
     TX          = collection category (RT for real-time; T1 for Tier 1;
                                        T2 for Tier 2)
     BN          = file type:
          B1         = band 1
          B2         = band 2
          B3         = band 3
          B4         = band 4
          B5         = band 5
          B6_VCID_1  = band 6L (low gain)  (ETM+)
          B6_VCID_2  = band 6H (high gain) (ETM+)
          B6         = band 6 (TM and MSS)
          B7         = band 7
          B8         = band 8 (ETM+)
          MTL        = Level-1 metadata
          GCP        = ground control points
     TIF         = GeoTIFF file extension

The file naming convention for Landsat 4-5 NLAPS-processed GeoTIFF data
is as follows:

LLNppprrrOOYYDDDMM_AA.TIF  where:
     LL          = Landsat sensor (LT for TM data)
     N           = satellite number
     ppp         = starting path of the product
     rrr         = starting row of the product
     OO          = WRS row offset (set to 00)
     YY          = last two digits of the year of
                   acquisition
     DDD         = Julian date of acquisition
     MM          = instrument mode (10 for MSS; 50 for TM)
     AA          = file type:
          B1          = band 1
          B2          = band 2
          B3          = band 3
          B4          = band 4
          B5          = band 5
          B6          = band 6
          B7          = band 7
          WO          = processing history file
     TIF         = GeoTIFF file extension
    """
    def __init__(self, row, base_mount_path='/imagery'):
        # self.__file_list = None
        # self.thread = threading.Thread(target=self.__query_file_list(), args=())
        # self.thread.daemon = True
        # self.thread.start()
        self.__wrs_geometries = WRSGeometries()

        # TODO we should flesh out the AWS from path instantiation
        if isinstance(row, str):
            self.__construct_aws(row)
            self.spacecraft_id = SpacecraftID[self.spacecraft_id]
        elif isinstance(row, tuple):
            self.__construct(row, base_mount_path)
            self.spacecraft_id = SpacecraftID[self.spacecraft_id]
        else:
            self.__construct_grpc(row, base_mount_path)
            self.spacecraft_id = SpacecraftID(self.spacecraft_id)

        # calculated fields


        # TODO, test some AWS data that is sensed on one date and then processed at another
        self.doy = datetime.strptime(self.date_acquired, "%Y-%m-%d").timetuple().tm_yday

        self.__band_map = BandMap(self.spacecraft_id)

        # TODO dateline testing
        center_lat = (self.north_lat - self.south_lat) / 2 + self.south_lat
        center_lon = (self.east_lon - self.west_lon) / 2 + self.west_lon

        self.utm_epsg_code = self.get_utm_epsg_code(center_lon, center_lat)

        #  (minx, miny, maxx, maxy)
        self.bounds = (self.west_lon, self.south_lat, self.east_lon, self.north_lat)

    def __construct(self, row, base_mount_path):
        # TODO, this could use a shallow copy? instead of creating an object like this? And thne all the attributes
        # would just call the array indices?

        self.scene_id = row[0]  # STRING	REQUIRED   Unique identifier for a particular Landsat image downlinked to
        # a particular ground station.
        self.product_id = row[1]  # STRING	NULLABLE Unique identifier for a particular scene processed by the USGS at
        # a particular time, or null for pre-collection data.
        self.spacecraft_id = row[2].upper()  # SpacecraftID REQUIRED The spacecraft that acquired this
        # scene: one of 'LANDSAT_4' through 'LANDSAT_8'.
        self.sensor_id = row[3]  # STRING	NULLABLE The type of spacecraft sensor that acquired this scene: 'TM' for
        # the Thematic Mapper, 'ETM' for the Enhanced Thematic Mapper+, or 'OLI/TIRS' for the Operational Land Imager
        # and Thermal Infrared Sensor.

        self.date_acquired = row[4]  # STRING	NULLABLE The date on which this scene was acquired (UTC).
        self.sensing_time = row[5]  # STRING	NULLABLE The approximate time at which this scene was acquired (UTC).
        self.collection_number = row[6]  # STRING	NULLABLE The Landsat collection that this image belongs to, e.g.
        # '01' for Collection 1 or 'PRE' for pre-collection data.
        self.collection_category = row[7]  # STRING	NULLABLE Indicates the processing level of the image: 'RT' for
        # real-time, 'T1' for Tier 1, 'T2' for Tier 2, and 'N/A' for pre-collection data. RT images will be replaced
        # with Tier 1 or Tier 2 images as they become available.
        self.data_type = row[8]  # STRING	NULLABLE The type of processed image, e.g. 'L1T' for Level 1
        # terrain-corrected images.
        self.wrs_path = row[9]  # INTEGER	NULLABLE The path number of this scene's location in the Worldwide
        # Reference System (WRS).
        self.wrs_row = row[10]  # INTEGER	NULLABLE The row number of this scene's location in the Worldwide
        # Reference System (WRS).
        self.cloud_cover = row[11]  # FLOAT	NULLABLE Estimated percentage of this scene affected by cloud cover.
        self.north_lat = row[12]  # FLOAT	NULLABLE The northern latitude of the bounding box of this scene.
        self.south_lat = row[13]  # FLOAT	NULLABLE The southern latitude of the bounding box of this scene.
        self.west_lon = row[14]  # FLOAT	NULLABLE The western longitude of the bounding box of this scene.
        self.east_lon = row[15]  # FLOAT	NULLABLE The eastern longitude of the bounding box of this scene.
        self.total_size = row[16]  # INTEGER	NULLABLE The total size of this scene in bytes.
        self.base_url = row[17]  # STRING	NULLABLE The base URL for this scene in Cloud Storage.

        self.__base_mount_path = base_mount_path

        if PLATFORM_PROVIDER == "GCP":
            gsurl = urlparse(self.base_url)
            self.__bucket_name = gsurl[1]
            self.__data_prefix = gsurl[2]

            self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + self.__bucket_name + os.path.sep + self.__data_prefix.strip("\/")
        else:
            self.full_mount_path = self.get_aws_file_path()

    def __construct_grpc(self, metadata_message, base_mount_path):
        for key in metadata_message.DESCRIPTOR.fields:
            setattr(self, key.name, getattr(metadata_message, key.name))

        self.__base_mount_path = base_mount_path

        if PLATFORM_PROVIDER == "GCP":
            gsurl = urlparse(self.base_url)
            self.__bucket_name = gsurl[1]
            self.__data_prefix = gsurl[2]

            self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + self.__bucket_name + os.path.sep + self.__data_prefix.strip("\/")
        else:
            self.full_mount_path = self.get_aws_file_path()

    def __construct_aws(self, row):
        # TODO there should be Metadata class for AWS and GOOGLE?
        self.full_mount_path = row
        self.product_id = os.path.basename(self.full_mount_path)
        # we know this is Landsat 8
        self.spacecraft_id = SpacecraftID.LANDSAT_8.name

        reg_results_1 = self.metadata_reg.search(self.full_mount_path)
        self.wrs_path = int(reg_results_1.group(1))
        self.wrs_row = int(reg_results_1.group(2))
        self.data_type = reg_results_1.group(4)
        self.date_acquired = date(int(reg_results_1.group(5)), int(reg_results_1.group(6)),
                                  int(reg_results_1.group(7))).strftime("%Y-%m-%d")
        self.collection_category = reg_results_1.group(8)

        mtl_file_path = "{0}/{1}_MTL.json".format(self.full_mount_path, self.name_prefix)
        mtl = self.parse_mtl(mtl_file_path)

        # '16:18:27.0722979Z'
        sensing_time = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['SCENE_CENTER_TIME']

        # '2017-10-28'
        date_acquired = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['DATE_ACQUIRED']

        reg_results_2 = self.datetime_reg.search(date_acquired + "T" + sensing_time)
        date_acquired = reg_results_2.group(1) + reg_results_2.group(2) + reg_results_2.group(3)
        self.sensing_time = datetime.strptime(date_acquired, "%Y-%m-%dT%H:%M:%S.%fZ")
        self.date_acquired = self.sensing_time.date().isoformat()
        # '2017-11-08T23:42:51Z'
        self.cloud_cover = mtl['L1_METADATA_FILE']['IMAGE_ATTRIBUTES']['CLOUD_COVER']
        self.cloud_cover_land = mtl['L1_METADATA_FILE']['IMAGE_ATTRIBUTES']['CLOUD_COVER_LAND']
        self.date_processed = datetime.strptime(mtl['L1_METADATA_FILE']['METADATA_FILE_INFO']['FILE_DATE'],
                                                "%Y-%m-%dT%H:%M:%SZ")

        # TODO, this is a crummy estimate. should use WRS paths result or actually select the extremes (distortion in projection makes this incorrect)
        self.north_lat = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['CORNER_LL_LAT_PRODUCT']  # FLOAT	NULLABLE The northern latitude of the bounding box of this scene.
        self.south_lat = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['CORNER_UL_LAT_PRODUCT']  # FLOAT	NULLABLE The southern latitude of the bounding box of this scene.
        self.west_lon = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['CORNER_UL_LON_PRODUCT']  # FLOAT	NULLABLE The western longitude of the bounding box of this scene.
        self.east_lon = mtl['L1_METADATA_FILE']['PRODUCT_METADATA']['CORNER_UL_LON_PRODUCT']  # FLOAT	NULLABLE The eastern longitude of the bounding box of this scene.
        # self.sensing_time = datetime.combine(date(2011, 01, 01), datetime.time(10, 23))
        return

    @property
    def bucket_name(self):
        return self.__bucket_name

    @property
    def base_mount_path(self):
        return self.__base_mount_path

    @property
    def band_map(self):
        return self.__band_map

    @property
    def name_prefix(self):
        return self.scene_id if not self.product_id else self.product_id

    @property
    def center(self):
        return shape(self.get_wrs_polygon()).centroid

    @staticmethod
    def parse_mtl(mtl_file_name):
        with open(mtl_file_name) as mtl:
            json_str = str(mtl.read())
            return json.loads(json_str)

    def get_wrs_polygon(self):
        return self.__wrs_geometries.get_wrs_geometry(self.wrs_path, self.wrs_row)

    # TODO, probably remove this?
    def get_intersect_wkt(self, other_bounds):
        xmin = self.bounds[0] if self.bounds[0] > other_bounds[0] else other_bounds[0]
        ymin = self.bounds[1] if self.bounds[1] > other_bounds[1] else other_bounds[1]

        xmax = self.bounds[2] if self.bounds[2] < other_bounds[2] else other_bounds[2]
        ymax = self.bounds[3] if self.bounds[3] < other_bounds[3] else other_bounds[3]

        return "POLYGON (({0} {1}, {2} {1}, {2} {3}, {0} {3}, {0} {1}))".format(xmin, ymin, xmax, ymax)

    @staticmethod
    def get_utm_epsg_code(longitude, latitude):
        # TODO yield alternative if perfectly at 6 degree interval
        # TODO throw or wrap if longitude greater than 180 or less than -180

        # epsg code for N1 32601
        epsg_code = 32601
        if latitude < 0:
            # epsg code for S1 32701
            epsg_code += 100

        diff = longitude + 180

        # TODO ugly
        if diff == 0:
            return epsg_code

        # 6 degrees of separation between zones, started with zone one, so subtract 1
        bump = int(math.ceil(diff / 6)) - 1

        return epsg_code + bump

    def get_file_list(self, timeout=4):
        # 4 second timeout on info
        # self.thread.join(timeout=timeout)
        # TODO if empty throw a warning?
        return []

    def get_aws_file_path(self):
        path = "/L8/{0}/{1}/".format(str(self.wrs_path).zfill(3), str(self.wrs_row).zfill(3))
        # PRE        s3://landsat-pds/L8/139/045/LC81390452014295LGN00/
        # non-PRE s3://landsat-pds/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1/
        if self.collection_number != "PRE":
            partial = self.product_id[:25]
            search = glob.glob(self.__base_mount_path + "/c1" + path + partial + "*")
            if len(search) == 0:
                # there is a potential situation where AWS has processed the PRE and removed PRE data but google only has PRE
                partial = self.scene_id[:16]
                search = glob.glob(self.__base_mount_path + path + partial + "*")
                if len(search) == 0:
                    # TODO instead of raising an exception maybe I need to handle this differently?
                    details = "glob returned {0} results for the following path search {1}".format(len(search), partial)
                    raise FileNotFoundError(details)
            if len(search) > 1:
                print("retrieved more than one entry. for {0} from method call 'get_aws_file_path'".format(partial))
            # update the product_id
            self.product_id = search[0].split("/")[-1]

            # update Collection category
            self.collection_category = self.product_id[-2:]

            return search[0]
        else:
            path = self.__base_mount_path + path + self.scene_id

        return path

    def get_full_file_path(self, band_number):
        return "{0}/{1}_B{2}.TIF".format(self.full_mount_path, self.name_prefix, band_number)

    def __query_file_list(self):
        bucket = self.__storage_client.list_buckets(prefix=self.__bucket_name + self.__data_prefix)
        results = []
        for i in bucket:
            results.append(i)
        self.__file_list = results
        # def __get_file_list(self):
        #     self.__file_list = None


class __RasterMetadata:

    # TODO, maybe there should be setters and getters to prevent problems?
    def __init__(self, band_number: int=None, metadata: Metadata=None):
        self.projection = None
        self.proj_cs = None
        self.data_type = None

        self.x_src_size = None
        self.y_src_size = None
        self.x_dst_size = None
        self.y_dst_size = None

        self.x_dst_offset = 0
        self.y_dst_offset = 0
        self.x_src_offset = 0
        self.y_src_offset = 0
        self.geo_transform = None
        # self.data_id = None
        self.bounds = None
        if metadata:
            file_path = metadata.get_full_file_path(band_number)

            dataset = gdal.Open(file_path)

            self.data_type = gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType)

            self.x_src_size = dataset.RasterXSize
            self.y_src_size = dataset.RasterYSize
            self.x_dst_size = dataset.RasterXSize
            self.y_dst_size = dataset.RasterYSize

            self.projection = dataset.GetProjection()
            self.geo_transform = dataset.GetGeoTransform()
            # self.data_id = name_prefix

            del dataset

            xmin = self.geo_transform[0]
            ymax = self.geo_transform[3]
            # self.geo_transform[1] is positive
            xmax = xmin + self.x_src_size * self.geo_transform[1]
            # self.geo_transform[5] is negative
            ymin = ymax + self.y_src_size * self.geo_transform[5]
            self.bounds = xmin, ymin, xmax, ymax

            srs = osr.SpatialReference()
            wkt_text = self.projection
            # Imports WKT to Spatial Reference Object
            srs.ImportFromWkt(wkt_text)
            self.proj_cs = pyproj.Proj(srs.ExportToProj4())

            self.file_path = file_path

    def clip_by_boundary(self, other_bounds, other_cs=None):
        # TODO throw exception
        # if not self.geo_transform:
        #     return None

        other_bounds_projected = other_bounds

        if other_cs:
            xminproj, yminproj = pyproj.transform(other_cs, self.proj_cs, other_bounds[0], other_bounds[1])
            xmaxproj, ymaxproj = pyproj.transform(other_cs, self.proj_cs, other_bounds[2], other_bounds[3])
            other_bounds_projected = xminproj, yminproj, xmaxproj, ymaxproj

        old_bounds = self.bounds
        old_xsize = self.x_src_size
        old_ysize = self.y_src_size
        xmin = old_bounds[0] if old_bounds[0] > other_bounds_projected[0] else other_bounds_projected[0]
        ymin = old_bounds[1] if old_bounds[1] > other_bounds_projected[1] else other_bounds_projected[1]

        xmax = old_bounds[2] if old_bounds[2] < other_bounds_projected[2] else other_bounds_projected[2]
        ymax = old_bounds[3] if old_bounds[3] < other_bounds_projected[3] else other_bounds_projected[3]

        calculated_bounds = xmin, ymin, xmax, ymax

        """
        http://www.gdal.org/gdal_tutorial.html[
        In the particular, but common, case of a "north up" image without any rotation or shearing,
        the georeferencing transform takes the following form
        adfGeoTransform[0] /* top left x */
        adfGeoTransform[1] /* w-e pixel resolution */
        adfGeoTransform[2] /* 0 */
        adfGeoTransform[3] /* top left y */
        adfGeoTransform[4] /* 0 */
        adfGeoTransform[5] /* n-s pixel resolution (negative value) */"""
        calculated_geo_transform = (calculated_bounds[0],
                                    self.geo_transform[1],
                                    0,
                                    calculated_bounds[3],
                                    0,
                                    self.geo_transform[5])

        original_xdiff = old_bounds[2] - old_bounds[0]
        calculated_xdiff = calculated_bounds[2] - calculated_bounds[0]

        original_ydiff = old_bounds[3] - old_bounds[1]
        calculated_ydiff = calculated_bounds[3] - calculated_bounds[1]

        # TODO, seems this should always be ceiling? Also, the input extent should be adjusted to be withing the
        # interval of pixel width, so that there isn't a slight shift of pixels??
        self.x_dst_size = int(round((calculated_xdiff / original_xdiff) * old_xsize))
        self.y_dst_size = int(round((calculated_ydiff / original_ydiff) * old_ysize))

        # This can be a float
        self.x_src_offset = ((calculated_bounds[0] - old_bounds[0]) / original_xdiff) * old_xsize
        self.y_src_offset = -((calculated_bounds[3] - old_bounds[3]) / original_ydiff) * old_ysize

        # MUST BE CALCULATED LAST SO AS NOT TO RUIN ABOVE OFFSET AND SIZE CALCULATIONS
        self.geo_transform = calculated_geo_transform
        self.bounds = calculated_bounds


class RasterBandMetadata(__RasterMetadata):

    def __init__(self,
                 band_number,
                 metadata: Metadata=None):
        super().__init__(band_number, metadata)
        self.band_number = band_number

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value


class RasterMetadata(__RasterMetadata):

    __wgs84_cs = pyproj.Proj(init='epsg:4326')

    def __init__(self):
        super().__init__()
        # TODO there needs to be a test to make sure that all of these items are length 1. They shouldn't be different, right?
        self.raster_band_metadata = {}
        self.__calculated = {}

    def clip_by_boundary(self, other_bounds, other_cs=None):
        super().clip_by_boundary(other_bounds, other_cs)
        for key in self.raster_band_metadata:
            self.raster_band_metadata[key].clip_by_boundary(other_bounds, other_cs)

    # TODO, this is going to be problematic when data sources have different source sizes, datatypes, etc.
    def add_metadata(self, band_number: int, metadata: Metadata):
        if band_number in self.raster_band_metadata:
            return

        self.raster_band_metadata[band_number] = RasterBandMetadata(band_number, metadata)

        # update RasterMetadata values according to the RasterBandMetadata
        for key, value in self.raster_band_metadata[band_number]:
            if key in ['file_path', 'band_number']:
                continue
            self_value = getattr(self, key)
            if key in ['proj_cs'] and self_value:
                continue
            # if not None and not equal
            if self_value and self_value != value:
                raise Exception("key {0} differs between RasterBandMetadata and RasterMetadata\nRasterBandMetadata: "
                                "{1}\nRasterBand: {2}\n".format(key, value, self_value))
            if not self_value:
                setattr(self, key, getattr(self.raster_band_metadata[band_number], key))

    def get_metadata(self, band_number) -> RasterBandMetadata:
        return self.raster_band_metadata[band_number]

    def contains(self, band_number):
        return band_number in self.raster_band_metadata

    def calculate_clipped(self, extent, extent_cs=None):
        if extent in self.__calculated:
            return self.__calculated[extent]

        # TODO override deep copy to copy each of the bands? Or store clipped information on bands and rasters instead of making a whole object copy?
        copied_raster = copy.deepcopy(self)

        if not extent_cs:
            extent_cs = self.__wgs84_cs
        copied_raster.clip_by_boundary(extent, extent_cs)
        self.__calculated[extent] = copied_raster
        return copied_raster


class Imagery:
    bucket_name = ""
    base_mount_path = ""
    storage = None

    # def __init__(self, base_mount_path, bucket_name="gcp-public-data-landsat"):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.storage = Storage(self.bucket_name)


class Landsat(Imagery):

    # def __init__(self, base_mount_path, bucket_name="gcp-public-data-landsat"):
    __metadata = None
    __id = None

    def __init__(self, metadata: [Metadata]):
        bucket_name = "gcp-public-data-landsat"
        super().__init__(bucket_name)
        if isinstance(metadata, list):
            self.__metadata = metadata
        else:
            self.__metadata = [metadata]
        self.__id = id(self)

    def __del__(self):
        for metadata in self.__metadata:
            self.storage.unmount_sub_folder(metadata, request_key=str(self.__id))

    def __calculate_metadata(self,
                             metadata: Metadata,
                             band_definitions: list,
                             extent: tuple=None,
                             extent_cs=None) -> RasterMetadata:
        # (in case one is calculated from a band that's included elsewhere in the metadata)
        band_number_set = set()
        for band_definition in band_definitions:
            if isinstance(band_definition, FunctionDetails):
                for band_number in band_definition.band_definitions:
                    if isinstance(band_number, Band):
                        band_number_set.add(metadata.band_map.get_number(band_number))
                    else:
                        band_number_set.add(band_number)
            elif isinstance(band_definition, Band):

                # TODO, something more pleasant please
                if band_definition is Band.ALPHA:
                    continue
                # TODO, something more pleasant please

                band_number_set.add(metadata.band_map.get_number(band_definition))
            else:
                band_number_set.add(band_definition)
        # All this does is convert band definitions band and band enums to numbers in a set
        # (in case one is calculated from a band that's included elsewhere in the metadata)

        # TODO do not create RasterMetadata object each time. hold a hash of them
        raster = RasterMetadata()

        for band_number in band_number_set:
            # TODO test force update
            if raster.contains(band_number): #  and not force_update:
                continue

            raster.add_metadata(band_number, metadata)

        if not extent:
            return raster

        return raster.calculate_clipped(extent=extent, extent_cs=extent_cs)

    def fetch_imagery_array(self,
                            band_definitions,
                            scale_params=None,
                            polygon_boundary_wkb: bytes=None,
                            envelope_boundary: tuple=None,
                            boundary_cs=4326,
                            output_type: DataType=DataType.BYTE,
                            spatial_resolution_m=60) -> np.ndarray:
        # TODO remove this, right?
        if polygon_boundary_wkb:
            envelope_boundary = shapely.wkb.loads(polygon_boundary_wkb).bounds

        dataset = self.get_dataset(band_definitions,
                                   output_type=output_type,
                                   scale_params=scale_params,
                                   envelope_boundary=envelope_boundary,
                                   polygon_boundary_wkb=polygon_boundary_wkb,
                                   spatial_resolution_m=spatial_resolution_m)
        nda = dataset.ReadAsArray()
        del dataset
        
        if len(band_definitions) >= 3:
            return nda.transpose((1, 2, 0))
        return nda

    def __get_source_elem(self,
                          band_number,
                          calculated_metadata:
                          RasterMetadata,
                          block_size=256):
        elem_simple_source = etree.Element("SimpleSource")

        # if the input had multiple bands this setting would be where you change that
        # but the google landsat is one tif per band
        etree.SubElement(elem_simple_source, "SourceBand").text = str(1)

        raster_band_metadata = calculated_metadata.get_metadata(band_number)

        elem_source_filename = etree.SubElement(elem_simple_source, "SourceFilename")
        elem_source_filename.set("relativeToVRT", "0")
        elem_source_filename.text = raster_band_metadata.file_path

        elem_source_props = etree.SubElement(elem_simple_source, "SourceProperties")
        elem_source_props.set("RasterXSize", str(raster_band_metadata.x_src_size))
        elem_source_props.set("RasterYSize", str(raster_band_metadata.y_src_size))
        elem_source_props.set("DataType", raster_band_metadata.data_type)

        # there may be a more efficient size than 256
        elem_source_props.set("BlockXSize", str(block_size))
        elem_source_props.set("BlockYSize", str(block_size))

        elem_src_rect = etree.SubElement(elem_simple_source, "SrcRect")
        elem_src_rect.set("xOff", str(raster_band_metadata.x_src_offset))
        elem_src_rect.set("yOff", str(raster_band_metadata.y_src_offset))
        elem_src_rect.set("xSize", str(raster_band_metadata.x_src_size))
        elem_src_rect.set("ySize", str(raster_band_metadata.y_src_size))

        elem_dst_rect = etree.SubElement(elem_simple_source, "DstRect")
        elem_dst_rect.set("xOff", str(raster_band_metadata.x_dst_offset))
        elem_dst_rect.set("yOff", str(raster_band_metadata.y_dst_offset))
        elem_dst_rect.set("xSize", str(raster_band_metadata.x_src_size))
        elem_dst_rect.set("ySize", str(raster_band_metadata.y_src_size))

        return elem_simple_source

    def __get_function_band_elem(self,
                                 vrt_dataset: etree.Element,
                                 band_definition: FunctionDetails,
                                 position_number,
                                 calculated_metadata,
                                 metadata,
                                 block_size=256):
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")
        # data_type = gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType)
        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        elem_raster_band.set("dataType", band_definition.data_type.name)
        elem_raster_band.set("band", str(position_number))
        elem_raster_band.set("subClass", "VRTDerivedRasterBand")

        # elem_simple_source = etree.SubElement(elem_raster_band, "SimpleSource")

        etree.SubElement(elem_raster_band, "PixelFunctionLanguage").text = "Python"
        etree.SubElement(elem_raster_band, "PixelFunctionType").text = band_definition.name

        # if transfer type is defined and it's not unknown
        if band_definition.transfer_type and band_definition.transfer_type is not DataType.UNKNOWN_GDAL:
            etree.SubElement(elem_raster_band, "SourceTransferType").text = band_definition.transfer_type.name

        if band_definition.code:
            etree.SubElement(elem_raster_band, "PixelFunctionCode").text = etree.CDATA(band_definition.code)

        if band_definition.arguments:
            # <PixelFunctionArguments factor="1.5"/>
            etree.SubElement(elem_raster_band, "PixelFunctionArguments", attrib=band_definition.arguments)
            # for function_arg_key in band_definition.arguments:
            #     elem_function_args.set(function_arg_key, str(band_definition.arguments[function_arg_key]))

        for band_number in band_definition.band_definitions:
            # TODO, I don't like this reuse of this variable
            if isinstance(band_number, Band):
                band_number = metadata.band_map.get_number(band_number)

            elem_simple_source = self.__get_source_elem(band_number, calculated_metadata, block_size)
            elem_raster_band.append(elem_simple_source)

    def __get_band_elem(self,
                        vrt_dataset,
                        band_number,
                        position_number,
                        calculated_metadata: RasterMetadata,
                        metadata,
                        block_size=256):
        # I think this needs to be removed.
        color_interp = metadata.band_map.get_name(band_number).capitalize()

        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        if color_interp:
            etree.SubElement(elem_raster_band, "ColorInterp").text = color_interp

        elem_simple_source = self.__get_source_elem(band_number, calculated_metadata, block_size)
        elem_raster_band.append(elem_simple_source)

        elem_raster_band.set("dataType", calculated_metadata.get_metadata(band_number).data_type)
        elem_raster_band.set("band", str(position_number))

    def get_vrt(self,
                band_definitions: list,
                metadata: Metadata=None,
                translate_args=None,
                envelope_boundary: tuple=None,
                boundary_cs: pyproj.Proj=None,
                spatial_resolution_m=60):
        # TODO remove this check, make Metadata a mandatory input
        if not metadata:
            metadata = self.__metadata[0]
        # TODO remove this check, make Metadata a mandatory input

        # TODO move this under __init__? Maybe run it on a separate thread
        if self.storage.mount_sub_folder(metadata, request_key=str(self.__id)) is False:
            return None

        vrt_dataset = etree.Element("VRTDataset")

        position_number = 1

        # self.get_band_metadata(band_definitions)
        calculated_metadata = self.__calculate_metadata(metadata, band_definitions, extent=envelope_boundary, extent_cs=boundary_cs)
        geo_transform = calculated_metadata.geo_transform
        etree.SubElement(vrt_dataset, "GeoTransform").text = ",".join(map("  {:.16e}".format, geo_transform))
        vrt_dataset.set("rasterXSize", str(calculated_metadata.x_dst_size))
        vrt_dataset.set("rasterYSize", str(calculated_metadata.y_dst_size))
        etree.SubElement(vrt_dataset, "SRS").text = calculated_metadata.projection

        # TODO if no bands throw exception
        for band_definition in band_definitions:
            if isinstance(band_definition, FunctionDetails):
                self.__get_function_band_elem(vrt_dataset,
                                              band_definition,
                                              position_number,
                                              calculated_metadata,
                                              metadata,
                                              256)

            elif isinstance(band_definition, Band):
                # TODO, something more pleasant please
                if band_definition is Band.ALPHA:
                    continue
                # TODO, something more pleasant please

                self.__get_band_elem(vrt_dataset,
                                     metadata.band_map.get_number(band_definition),
                                     position_number,
                                     calculated_metadata,
                                     metadata,
                                     256)

            else:
                self.__get_band_elem(vrt_dataset,
                                     band_definition,
                                     position_number,
                                     calculated_metadata,
                                     metadata,
                                     256)

            position_number += 1

        return etree.tostring(vrt_dataset, encoding='UTF-8', method='xml')

    def __get_translated_datasets(self,
                                  band_definitions,
                                  output_type: DataType,
                                  scale_params=None,
                                  envelope_boundary: tuple=None,
                                  xRes=60,
                                  yRes=60):
        translated = []
        for metadata in self.__metadata:
            if self.storage.mount_sub_folder(metadata, request_key=str(self.__id)) is False:
                return None

            # TODO, the envelope requested should be a part of the metadata, so that the envelope
            # boundary can be pulled from that if available
            vrt = self.get_vrt(band_definitions, metadata=metadata, envelope_boundary=envelope_boundary)
            # http://gdal.org/python/
            # http://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
            dataset_translated = gdal.Translate('', vrt.decode('utf-8'),
                                                format='MEM',
                                                scaleParams=scale_params,
                                                xRes=xRes,
                                                yRes=yRes,
                                                outputType=output_type.gdal,
                                                noData=0)
            translated.append(dataset_translated)
        return translated

    def get_dataset(self,
                    band_definitions,
                    output_type: DataType,
                    scale_params=None,
                    envelope_boundary: tuple = None,
                    polygon_boundary_wkb: bytes = None,
                    spatial_resolution_m=60):
        dataset_translated = self.__get_translated_datasets(band_definitions,
                                                            output_type,
                                                            scale_params,
                                                            envelope_boundary,
                                                            xRes=spatial_resolution_m,
                                                            yRes=spatial_resolution_m)

        b_alpha_channel = Band.ALPHA in band_definitions
        # if there is no need to warp the data
        if not polygon_boundary_wkb and len(dataset_translated) == 1 and not b_alpha_channel:
            return dataset_translated[0]

        dataset_warped = self.__get_warped(dataset_translated,
                                           output_type=output_type,
                                           polygon_boundary_wkb=polygon_boundary_wkb,
                                           dstAlpha=b_alpha_channel)

        for dataset in dataset_translated:
            del dataset

        return dataset_warped

    def __get_warped(self,
                     dataset_translated: ogr,
                     output_type: DataType,
                     polygon_boundary_wkb: bytes=None,
                     dstAlpha: bool=False):
        cutlineDSName = None
        if polygon_boundary_wkb:
            cutlineDSName = '/vsimem/cutline.json'
            cutline_ds = ogr.GetDriverByName('GeoJSON').CreateDataSource(cutlineDSName)
            cutline_lyr = cutline_ds.CreateLayer('cutline')
            f = ogr.Feature(cutline_lyr.GetLayerDefn())

            f.SetGeometry(ogr.CreateGeometryFromWkb(polygon_boundary_wkb))
            cutline_lyr.CreateFeature(f)
            f = None
            cutline_lyr = None
            cutline_ds = None

        dataset_warped = gdal.Warp("",
                                   dataset_translated,
                                   format='MEM',
                                   multithread=True,
                                   cutlineDSName=cutlineDSName,
                                   outputType=output_type.gdal,
                                   dstAlpha=dstAlpha)


        return dataset_warped


class Sentinel2:
    bucket_name = ""


class MetadataService(metaclass=__Singleton):


    """
    Notes on WRS-2 Landsat 8's Operational Land Imager (OLI) and/or Thermal Infrared Sensor (TIRS) sensors acquired
    nearly 10,000 scenes from just after its February 11, 2013 launch through April 10, 2013, during when the
    satellite was moving into the operational WRS-2 orbit. The earliest images are TIRS data only.  While these data
    meet the quality standards and have the same geometric precision as data acquired on and after April 10, 2013,
    the geographic extents of each scene will differ. Many of the scenes are processed to full terrain correction,
    with a pixel size of 30 meters. There may be some differences in the spatial resolution of the early TIRS images
    due to telescope temperature changes.
    """

    def __init__(self):
        self.m_client = bigquery.Client()
        self.m_wrs_geometry = WRSGeometries()
        self.m_timeout_ms = 10000

    # @staticmethod
    # def get_aws_landsat_path(wrs_path,
    #                          wrs_row,
    #                          collection_number,
    #                          product_id=None,
    #                          scene_id=None,
    #                          acq_year=None,
    #                          acq_month=None,
    #                          acq_day=None):
    #
    #
    #     return path

    @staticmethod
    def _dateline_intersection(coord_pair1, coord_pair2):
        result = None
        lon_diff = coord_pair1[0] - coord_pair2[0]
        if lon_diff > 180:
            pos_lon = 360 + coord_pair2[0]
            pos_diff = pos_lon - coord_pair1[0]
            ratio = (pos_lon - 180) / pos_diff
            lat_intersect = coord_pair1[1] + (coord_pair2[1] - coord_pair1[1]) * ratio
            result = [(180, lat_intersect), (-180, lat_intersect)]
        elif lon_diff < -180:
            result = MetadataService._dateline_intersection(coord_pair2, coord_pair1)
            result.reverse()

        return result

    @staticmethod
    def split_by_dateline(poly: shapely.geometry.Polygon) -> [shapely.geometry.Polygon]:
        if not isinstance(poly, shapely.geometry.Polygon):
            raise ValueError

        # if it is ccw then it's arranged correctly
        if poly.exterior.is_ccw:
            return [poly]

        xmin = poly.bounds[0]
        xmax = poly.bounds[2]
        coords_positive = []
        coords_negative = []

        # if the bounds are negative and positive rearrange, otherwise don't
        if xmin < 0 < xmax:
            coords = [p for p in poly.exterior.coords]

            for index, coord_pair in enumerate(coords):

                next_coord = None
                if index == len(coords) - 1:
                    next_coord = coords[0]
                else:
                    next_coord = coords[index + 1]

                # TODO so, if a geometry starts in the negative longitude region and eventually comes into the positive space, this will break.
                if coord_pair[0] > 0:
                    coords_positive.append(coord_pair)
                    intersection_coords = MetadataService._dateline_intersection(coord_pair, next_coord)
                    if intersection_coords:
                        coords_positive.append(intersection_coords[0])
                        coords_negative.append(intersection_coords[1])
                else:
                    coords_negative.append(coord_pair)
                    intersection_coords = MetadataService._dateline_intersection(coord_pair, next_coord)
                    if intersection_coords:
                        coords_negative.append(intersection_coords[0])
                        coords_positive.append(intersection_coords[1])

            return [shapely.geometry.Polygon(coords_positive), shapely.geometry.Polygon(coords_negative)]

        return [poly]

    @staticmethod
    def split_all_by_dateline(polygon_wkbs: List[bytes]):
        """
        With a list of polygons encoding in wkb, return a list of shapely polygons that are split by the dateline if
        necessary. All inputs and output are assumed to be in Geographic Coordinates (wgs-84 or other lon,lat system)
        :param polygon_wkbs:
        :return: list of polygons split by dateline if necessary
        """
        results = []

        for polygon_wkb in polygon_wkbs:
            geom = shapely.wkb.loads(polygon_wkb)
            if geom.type == "MultiPolygon":
                for p in geom.geoms:
                    results.extend(MetadataService.split_by_dateline(p))
            elif geom.type == "Polygon":
                results.extend(MetadataService.split_by_dateline(geom))
            else:
                raise ValueError

        return results

    def bounds_from_multipolygon(self, multi_polygon):
        results = []
        if multi_polygon.type == "MultiPolygon":
            for p in multi_polygon.geoms:
                results.append(p.bounds)
        elif multi_polygon.type == "Polygon":
            results = [multi_polygon.bounds]
        else:
            raise ValueError

        return results

    def get_wrs(self, polygon_wkbs: List[bytes], search_area_unioned):
        polygon_shapes = MetadataService.split_all_by_dateline(polygon_wkbs)
        wrs_set = set()
        for poly in polygon_shapes:
            temp_set = self.m_wrs_geometry.get_path_row(poly.bounds)
            for temp_wrs_pair in temp_set:
                wrs_geometry = shapely.wkb.loads(self.m_wrs_geometry.get_wrs_geometry(wrs_row=temp_wrs_pair[1], wrs_path=temp_wrs_pair[0]))
                if wrs_geometry.intersects(search_area_unioned):
                    wrs_set.add(temp_wrs_pair)

        return list(wrs_set)

    def sorted_wrs_overlaps(self, wrs_set, search_area):
        wrs_overlaps = []
        for temp_wrs_pair in wrs_set:
            wrs_geometry = shapely.wkb.loads(
            self.m_wrs_geometry.get_wrs_geometry(wrs_row=temp_wrs_pair[1], wrs_path=temp_wrs_pair[0]))
            inserecting_area = wrs_geometry.intersection(search_area).area
            if inserecting_area > 0:
                wrs_overlaps.append((*temp_wrs_pair, inserecting_area))

        return sorted(wrs_overlaps, key=itemgetter(2), reverse=False)


    @staticmethod
    def get_search_area(data_filters: MetadataFilters=None) -> shapely.geometry:
        # TODO project inputs to WGS84 before
        search_area_polygon = None
        if data_filters.aoi.query_params.geometry_bag.geometry_binaries:
            search_area_polygon = shapely.geometry.Polygon()

            # TODO, this right here is an example of why there should be something beyond geometry_binaries and the use of an enum.
            for polygon_wkb in data_filters.aoi.query_params.geometry_bag.geometry_binaries:
                temp_polygon = shapely.wkb.loads(polygon_wkb)
                bounding_box = temp_polygon.bounds
                data_filters.aoi.set_bounds(*bounding_box)
                search_area_polygon = search_area_polygon.union(temp_polygon)

        elif data_filters.aoi.b_initialized:
            search_area_polygon = shapely.geometry.Polygon()
            for bounding_box in data_filters.aoi.query_params.bounds:
                search_area_polygon = search_area_polygon.union(
                    shapely.geometry.box(
                        bounding_box.xmin,
                        bounding_box.ymin,
                        bounding_box.xmax,
                        bounding_box.ymax).envelope)

        return search_area_polygon

    @staticmethod
    def search_aws(mount_base_path,
                   wrs_path,
                   wrs_row,
                   collection_date: datetime,
                   processing_level: str="L1TP"):
        """This is basically a hack for when data is not in BigQuery for the first 24 hours that the data exists
             LLLL        = processing level (L1TP for Precision Terrain;
                                     L1GT for Systematic Terrain;
                                     L1GS for Systematic only)
        """
        # check that the request is for data that is at most within the last 24 hours

        # build a glob string
        # Shouldn't be PRE as
        # PRE        s3://landsat-pds/L8/139/045/LC81390452014295LGN00/

        # non-PRE s3://landsat-pds/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1/
        path = "{0}/c1/L8/{1}/{2}/".format(mount_base_path, str(wrs_path).zfill(3), str(wrs_row).zfill(3))

        # a = '(LC08_[\w]+_{0}{1}_{2}{3}{4}).*$'.format(str(wrs_path).zfill(3), str(wrs_row).zfill(3),
        #                                               collection_date.year, str(collection_date.month).zfill(2),
        #                                               str(collection_date.day).zfill(2))

        directory = 'LC08_{0}_{1}{2}_{3}{4}{5}_*'.format(processing_level,
                                                         str(wrs_path).zfill(3),
                                                         str(wrs_row).zfill(3),
                                                         collection_date.year,
                                                         str(collection_date.month).zfill(2),
                                                         str(collection_date.day).zfill(2))

        paths = glob.glob(path + directory)
        metadata_rows = []
        for row in paths:
            metadata_rows.append(Metadata(row, mount_base_path))
        return metadata_rows

    def search(self,
               satellite_id=None,
               limit=10,
               data_filters: MetadataFilters=None,
               base_mount_path='/imagery') -> Generator[Metadata, None, None]:

        if PLATFORM_PROVIDER == 'AWS':
            # this is really arbitrary, but we're saying any data before mid-year 2013 should be excluded
            # from searches. This could be refined, but really, if you want data from before then use google.
            exclude_data_before = date(2013, 7, 1)
            data_filters.acquired.set_exclude_range(end=exclude_data_before)

        if not data_filters:
            data_filters = LandsatQueryFilters()

        if satellite_id and satellite_id is not SpacecraftID.UNKNOWN_SPACECRAFT:
            data_filters.spacecraft_id.set_value(satellite_id.name)

        search_area_polygon = self.get_search_area(data_filters=data_filters)

        limit_found = 0
        query_string = data_filters.get_sql(limit=limit)
        b_limit_reached = False
        while limit_found < limit:
            # TODO sort by area

            exclude_scene_id = []
            exclude_product_id = []

            # TODO update to use bigquery asynchronous query.
            query = self.m_client.run_sync_query(query_string)
            query.timeout_ms = self.m_timeout_ms

            # TODO this should moved into a method by itself and handled more elegantly
            try:
                query.run()
            except exceptions.GoogleCloudError:
                try:
                    query.run()
                    Warning("exceptions.GoogleCloudError:", sys.exc_info()[0])
                except exceptions.GoogleCloudError:
                    raise
            except ValueError:
                raise

            if len(query.rows) == 0:
                return
            elif len(query.rows) < limit:
                b_limit_reached = True

            for row in query.rows:
                try:
                    metadata = Metadata(row, base_mount_path)
                except FileNotFoundError:
                    Warning("scene {0} / product {1} not found".format(row.scene_id, row.product_id))
                    continue

                if search_area_polygon is None or search_area_polygon.is_empty:
                    exclude_scene_id.append(metadata.scene_id)
                    exclude_product_id.append(metadata.product_id)
                    limit_found += 1
                    yield metadata
                    continue

                wrs_wkb = self.m_wrs_geometry.get_wrs_geometry(wrs_path=metadata.wrs_path, wrs_row=metadata.wrs_row)
                wrs_shape = shapely.wkb.loads(wrs_wkb)
                if wrs_shape.intersects(search_area_polygon):
                    exclude_scene_id.append(metadata.scene_id)
                    exclude_product_id.append(metadata.product_id)
                    limit_found += 1
                    yield metadata

                if limit_found >= limit:
                    break

            if b_limit_reached:
                break

            if limit_found < limit:
                for scene_id in exclude_scene_id:
                    if scene_id:
                        data_filters.scene_id.set_exclude_value(scene_id)
                for product_id in exclude_product_id:
                    if product_id:
                        data_filters.product_id.set_exclude_value(product_id)
                query_string = data_filters.get_sql(limit=limit)

    def _layer_group_by_area(self,
                             data_filters_copy: LandsatQueryFilters,
                             search_area_polygon,
                             wrs_intersections=None,
                             satellite_id=None,
                             by_area=False):

        loop_var = search_area_polygon.area
        limit = 10
        if by_area:
            # this seems not intuitive, since this is by area, but this is for the best
            loop_var = len(wrs_intersections)
            limit = 1

        while loop_var > 0:

            if by_area:
                wrs_intersections_sorted = self.sorted_wrs_overlaps(wrs_intersections, search_area_polygon)
                loop_var = len(wrs_intersections_sorted)
                wrs_details = wrs_intersections_sorted.pop()
                wrs_intersections.remove((wrs_details[0], wrs_details[1]))

                # TODO get consistent about wrs_pair order
                data_filters_copy.wrs_path_row.set_pair(wrs_details[0], wrs_details[1])

            sort_value = None
            for metadata in self.search(satellite_id=satellite_id, limit=limit, data_filters=data_filters_copy):
                wrs_shape = shapely.wkb.loads(self.m_wrs_geometry.get_wrs_geometry(wrs_row=metadata.wrs_row, wrs_path=metadata.wrs_path))
                wrs_poly_intersection = wrs_shape.intersection(search_area_polygon)
                wrs_poly_intersection = wrs_poly_intersection.buffer(0.00000008)
                previous_area = search_area_polygon.area
                search_area_polygon = search_area_polygon.difference(wrs_poly_intersection)

                if previous_area > search_area_polygon.area:
                    yield metadata

                if search_area_polygon.area <= 0:
                    return

                sort_value = metadata.__dict__[data_filters_copy.sorted_by.field.name]

            if by_area:
                # reset the query_params (or we could do another deep copy, but that seems bad
                data_filters_copy.wrs_path_row.query_params.values.pop()
                data_filters_copy.wrs_path_row.query_params.values.pop()
            else:
                if sort_value and data_filters_copy.sorted_by.query_params.sort_direction == epl_imagery_pb2.DESCENDING:
                    data_filters_copy.sorted_by.set_exclude_range(start=sort_value)
                elif sort_value and data_filters_copy.sorted_by.query_params.sort_direction == epl_imagery_pb2.ASCENDING:
                    data_filters_copy.sorted_by.set_exclude_range(end=sort_value)
                loop_var = search_area_polygon.area
                data_filters_copy.aoi.query_params.ClearField("bounds")
                for bounds in self.bounds_from_multipolygon(search_area_polygon):
                    data_filters_copy.aoi.set_bounds(*bounds)

    def search_layer_group(self,
                           data_filters: LandsatQueryFilters,
                           satellite_id=None):
        polygon_wkbs = []

        polygon, sr_data = data_filters.aoi.get_geometry()
        if polygon:
            polygon_wkbs = data_filters.aoi.geometry_bag.geometry_binaries
        elif data_filters.aoi:
            for bounding_box in data_filters.aoi.query_params.bounds:
                polygon_wkbs.append((shapely.geometry.box(bounding_box.xmin,
                                                          bounding_box.ymin,
                                                          bounding_box.xmax,
                                                          bounding_box.ymax).envelope).wkb)

        else:
            raise ValueError("must have a search area to create a layer group")

        search_area_polygon = self.get_search_area(data_filters=data_filters)

        if not data_filters.sorted_by:
            data_filters.acquired.sort_by(epl_imagery_pb2.DESCENDING)

        if data_filters.aoi.query_params.sort_direction != epl_imagery_pb2.NOT_SORTED:
            data_filters.acquired.sort_by(epl_imagery_pb2.DESCENDING)
            wrs_intersections = self.get_wrs(polygon_wkbs, search_area_polygon)
            data_filters_copy = copy.deepcopy(data_filters)
            data_filters_copy.aoi.query_params.ClearField("bounds")
            return self._layer_group_by_area(data_filters_copy=data_filters_copy,
                                             search_area_polygon=search_area_polygon,
                                             wrs_intersections=wrs_intersections,
                                             satellite_id=satellite_id,
                                             by_area=True)
        else:
            data_filters_copy = copy.deepcopy(data_filters)
            return self._layer_group_by_area(data_filters_copy=data_filters_copy,
                                             search_area_polygon=search_area_polygon,
                                             satellite_id=satellite_id,
                                             by_area=False)


class Storage(metaclass=__Singleton):

    bucket = ""
    __mounted_sub_folders = None

    def __init__(self, bucket_name="gcp-public-data-landsat"):
        self.bucket = bucket_name
        self.__mounted_sub_folders = {}

    def __del__(self):
        return
        # TODO, this is a gross way to skip Storage code on AWS
        if PLATFORM_PROVIDER == "AWS":
            return

        for full_path in self.__mounted_sub_folders:
            self.__unmount_sub_folder(full_path, "", force=True)

    def is_mounted(self, metadata: Metadata):
        return True
        # TODO, this is a gross way to skip Storage code on AWS
        if PLATFORM_PROVIDER == "AWS":
            return True

        if metadata.full_mount_path in self.__mounted_sub_folders and \
                self.__mounted_sub_folders[metadata.full_mount_path]:
            return True
        return False

    def mount_sub_folder(self, metadata: Metadata, request_key="temp"):
        return True
        # TODO, this is a gross way to skip Storage code on AWS
        if PLATFORM_PROVIDER == "AWS":
            return True

        # execute mount command
        # gcsfuse --only-dir LC08/PRE/044/034/LC80440342016259LGN00 gcp-public-data-landsat /landsat

        # full_mount_path = base_path.rstrip("\/") + os.path.sep + bucket_sub_folder.strip("\/")
        # subprocess.run("exit 1", shell=True, check=True)
        # subprocess.run(["ls", "-l", "/dev/null"], stdout=subprocess.PIPE)
        if metadata.full_mount_path in self.__mounted_sub_folders and \
                        request_key in self.__mounted_sub_folders[metadata.full_mount_path]:
            return True

        # This is a little weird. It could be that from a crash, some of the folders are already mounted by gcsfuse
        # this would set it so that those mountings were removed. If there are two Storage scripts running in parallel
        # on the same machine then maybe there would be conflicts. Not sure how to share those resources without
        # destroying them, but still cleaning up after them. Virtual files seems like the way to go with all this.
        if metadata.full_mount_path not in self.__mounted_sub_folders:
            self.__mounted_sub_folders[metadata.full_mount_path] = set()

        try:
            if not os.path.isdir(metadata.full_mount_path):
                os.makedirs(metadata.full_mount_path)
            else:
                # check to see if directory is already mounted if so maybe just return True?
                if len(os.listdir(metadata.full_mount_path)) > 0:
                    self.__mounted_sub_folders[metadata.full_mount_path].add(request_key)
                    return True
                # hard to know what to do if it's mounted and it's empty...
                # TODO make a test for that case

        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        val = call(["gcsfuse",
                    "--only-dir",
                    metadata.full_mount_path.lstrip(metadata.base_mount_path).lstrip("\/"),
                    self.bucket,
                    metadata.full_mount_path])
        # TODO return error message if necessary
        if val != 0:
            return False

        self.__mounted_sub_folders[metadata.full_mount_path].add(request_key)
        return True

    def __unmount_sub_folder(self, full_mount_path, request_key, force=False):
        return True
        # TODO, this is a gross way to skip Storage code on AWS
        if PLATFORM_PROVIDER == "AWS":
            return True

        # fusermount -u /path/to/mount/point
        if not force and (full_mount_path not in self.__mounted_sub_folders or request_key not in self.__mounted_sub_folders[full_mount_path]):
            return True

        # remove the request_key from
        self.__mounted_sub_folders[full_mount_path].discard(request_key)

        # if there are still members of the set then escape
        if not force and self.__mounted_sub_folders[full_mount_path]:
            return True

        # if there are no more references to this storage object, unmount
        # of if this is being forced
        val = call(["fusermount", "-u", full_mount_path])
        if val != 0:
            return False

        # if the set is empty, maybe we reclaim the space in the hashmap
        if not self.__mounted_sub_folders[full_mount_path] or force:
            del self.__mounted_sub_folders[full_mount_path]

        return True

    def unmount_sub_folder(self, metadata, request_key, force=False):
        return self.__unmount_sub_folder(metadata.full_mount_path, request_key, force)


# TODO this could probably be moved into it's own file
class WRSGeometries(metaclass=__Singleton):

    """
Notes on WRS-2 Landsat 8's Operational Land Imager (OLI) and/or Thermal Infrared Sensor (TIRS) sensors
acquired nearly 10,000 scenes from just after its February 11, 2013 launch through April 10, 2013, during
 when the satellite was moving into the operational WRS-2 orbit. The earliest images are TIRS data only.
 While these data meet the quality standards and have the same geometric precision as data acquired on and
 after April 10, 2013, the geographic extents of each scene will differ. Many of the scenes are processed to
 full terrain correction, with a pixel size of 30 meters. There may be some differences in the spatial resolution
 of the early TIRS images due to telescope temperature changes.
    """
    def __init__(self):
        self.__wrs2_map = {}
        self.__spatial_index = Index(bbox=(-180, -90, 180, 90))

        wrs2 = shapefile.Reader("/.epl/metadata/wrs/wrs2_asc_desc/wrs2_asc_desc.shp")
        wrs_path_idx = None
        wrs_row_idx = None
        for idx, field in enumerate(wrs2.fields):
            if field[0] == "PATH":
                wrs_path_idx = idx - 1
            elif field[0] == "ROW":
                wrs_row_idx = idx - 1

        # self.__wrs1_records = self.__wrs1.records()
        records = wrs2.records()
        for idx, record in enumerate(records):
            path_num = record[wrs_path_idx]
            row_num = record[wrs_row_idx]

            if path_num not in self.__wrs2_map:
                self.__wrs2_map[path_num] = {}

            s = wrs2.shape(idx)
            self.__wrs2_map[path_num][row_num] = shapely.geometry.shape(s.__geo_interface__).wkb
            if s.__geo_interface__["type"] == "MultiPolygon":
                multipart = shapely.geometry.shape(s.__geo_interface__)
                for geom in multipart:
                    self.__spatial_index.insert((path_num, row_num), geom.bounds)
            else:
                self.__spatial_index.insert((path_num, row_num), s.bbox)

        # do some async query to check if the danger_zone needs updating
        # self.__read_thread = threading.Thread(target=self.__read_shapefiles, args=())
        # self.__read_thread.daemon = True  # Daemonize thread
        # self.__read_thread.start()

    # def __read_shapefiles(self):
        # self.__wrs1 = shapefile.Reader("/.epl/metadata/wrs/wrs1_asc_desc/wrs1_asc_desc.shp")

    def get_path_row(self, bounds) -> set:
        return self.__spatial_index.intersect(bounds)

    def get_wrs_geometry(self, wrs_path, wrs_row):
        return self.__wrs2_map[wrs_path][wrs_row]


