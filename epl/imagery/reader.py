import os
import errno
import threading

import tempfile
import py_compile

import shapefile

# TODO replace with geometry
import shapely.wkb
# TODO replace with geometry

import math
import pyproj
import copy

import numpy as np

from osgeo import osr, ogr, gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum, IntEnum
from subprocess import call

# Imports the Google Cloud client library
from google.cloud import bigquery, storage


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


class SpacecraftID(IntEnum):
    LANDSAT_1_MSS = 1
    LANDSAT_2_MSS = 2
    LANDSAT_3_MSS = 4
    LANDSAT_123_MSS = 7
    LANDSAT_4_MSS = 8
    LANDSAT_5_MSS = 16
    LANDSAT_45_MSS = 24
    LANDSAT_4 = 32
    LANDSAT_5 = 64
    LANDSAT_45 = 96
    LANDSAT_7 = 128
    LANDSAT_8 = 256
    ALL = 512


class Band(Enum):
    # Crazy Values so that the Band.<ENUM>.value isn't used for anything
    ULTRA_BLUE = -1000
    BLUE = -2000
    GREEN = -3000
    RED = -4000
    NIR = -5000
    SWIR1 = -6000
    THERMAL = -7000
    SWIR2 = -8000
    PANCHROMATIC = -9000
    CIRRUS = -10000
    TIRS1 = -11000
    TIRS2 = -12000
    INFRARED2 = -13000
    INFRARED1 = -14000
    ALPHA = -15000


class DataType(Enum):
    # Byte, UInt16, Int16, UInt32, Int32, Float32, Float64, CInt16, CInt32, CFloat32 or CFloat64

    BYTE = (gdal.GDT_Byte, "Byte", 0, 255)
    INT16 = (gdal.GDT_Int16, "Int16", -32768, 32767)
    UINT16 = (gdal.GDT_UInt16, "UInt16", 0, 65535)
    INT32 = (gdal.GDT_Int32, "Int32", -2147483648, 2147483647)
    UINT32 = (gdal.GDT_UInt32, "UInt32", 0, 4294967295)
    FLOAT32 = (gdal.GDT_Float32, "Float32", -3.4E+38, 3.4E+38)
    FLOAT64 = (gdal.GDT_Float64, "Float64", -1.7E+308, 1.7E+308)
    CFLOAT32 = (gdal.GDT_CFloat32, "CFloat32", -3.4E+38, 3.4E+38)
    CFLOAT64 = (gdal.GDT_CFloat64, "CFloat64", -1.7E+308, 1.7E+308)

    def __init__(self, gdal_type, name, range_min, range_max):
        self.__gdal_type = gdal_type
        self.__name = name
        self.range_min = range_min
        self.range_max = range_max

    @property
    def gdal(self):
        return self.__gdal_type

    @property
    def name(self):
        return self.__name


class BandMap:
     # TODO it would be nice to store data type, Byte, Unit16, etc.
    __map = {
        SpacecraftID.LANDSAT_8: {
            'max_resolution': 30,
            Band.ULTRA_BLUE: {'number': 1, 'wavelength_range': (0.435, 0.451), 'description': 'Coastal and aerosol studies', 'resolution_m': 30},
            Band.BLUE: {'number': 2, 'wavelength_range': (0.452, 0.512), 'description': 'Bathymetric mapping, distinguishing soil from vegetation, and deciduous from coniferous vegetation', 'resolution_m': 30},
            Band.GREEN: {'number': 3, 'wavelength_range': (0.533, 0.590), 'description': 'Emphasizes peak vegetation, which is useful for assessing plant vigor', 'resolution_m': 30},
            Band.RED: {'number': 4, 'wavelength_range': (0.636, 0.673), 'description': 'Discriminates vegetation slopes', 'resolution_m': 30},
            Band.NIR: {'number': 5, 'wavelength_range': (0.851, 0.879), 'description': 'Emphasizes biomass content and shorelines', 'resolution_m': 30},
            Band.SWIR1: {'number': 6, 'wavelength_range': (1.566, 1.651), 'description': 'Discriminates moisture content of soil and vegetation; penetrates thin clouds', 'resolution_m': 30},
            Band.SWIR2: {'number': 7, 'wavelength_range': (2.107, 2.294), 'description': 'Improved moisture content of soil and vegetation and thin cloud penetration', 'resolution_m': 30},
            Band.PANCHROMATIC: {'number': 8, 'wavelength_range': (0.503, 0.676), 'description': '15 meter resolution, sharper image definition', 'resolution_m': 15},
            Band.CIRRUS: {'number': 9, 'wavelength_range': (1.363, 1.384), 'description': 'Improved detection of cirrus cloud contamination', 'resolution_m': 30},
            Band.TIRS1: {'number': 10, 'wavelength_range': (10.60, 11.19), 'description': '100 meter resolution, thermal mapping and estimated soil moisture', 'resolution_m': 30},
            Band.TIRS2: {'number': 11, 'wavelength_range': (11.50, 12.51), 'description': '100 meter resolution, Improved thermal mapping and estimated soil moisture', 'resolution_m': 30},
        },
        SpacecraftID.LANDSAT_45: {
            'max_resolution': 30,
            Band.BLUE: {'number': 1, 'wavelength_range': (0.45, 0.52), 'description': 'Bathymetric mapping, distinguishing soil from vegetation, and deciduous from coniferous vegetation', 'resolution_m': 30},
            Band.GREEN: {'number': 2, 'wavelength_range': (0.52, 0.60), 'description': 'Emphasizes peak vegetation, which is useful for assessing plant vigor', 'resolution_m': 30},
            Band.RED: {'number': 3, 'wavelength_range': (0.63, 0.69), 'description': 'Discriminates vegetation slopes', 'resolution_m': 30},
            Band.NIR: {'number': 4, 'wavelength_range': (0.77, 0.90), 'description': 'Emphasizes biomass content and shorelines', 'resolution_m': 30},
            Band.SWIR1: {'number': 5, 'wavelength_range': (1.55, 1.75), 'description': 'Discriminates moisture content of soil and vegetation; penetrates thin clouds', 'resolution_m': 30},
            Band.THERMAL: {'number': 6, 'wavelength_range': (10.40, 12.50), 'description': 'Thermal mapping and estimated soil moisture (60m downsample Landsat7, 120m downsample landsat 4&5)', 'resolution_m': 30},
            Band.SWIR2: {'number': 7, 'wavelength_range': (2.09, 2.35), 'description': 'Hydrothermally altered rocks associated with mineral deposits', 'resolution_m': 30},
        },
        SpacecraftID.LANDSAT_123_MSS:{
            'max_resolution': 60,
            Band.GREEN: {'number': 4, 'wavelength_range': (0.5, 0.6), 'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 5, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features', 'resolution_m': 60},
            Band.INFRARED1: {'number': 6, 'wavelength_range': (0.7, 0.8), 'description': 'Vegetation boundary between land and water, and landforms', 'resolution_m': 60},
            Band.INFRARED2: {'number': 7, 'wavelength_range': (0.8, 1.1), 'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms', 'resolution_m': 60},
        },
        SpacecraftID.LANDSAT_45_MSS: {
            'max_resolution': 60,
            Band.GREEN: {'number': 1, 'wavelength_range': (0.5, 0.6), 'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 2, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features', 'resolution_m': 60},
            Band.INFRARED1: {'number': 3, 'wavelength_range': (0.7, 0.8), 'description': 'Vegetation boundary between land and water, and landforms', 'resolution_m': 60},
            Band.INFRARED2: {'number': 4, 'wavelength_range': (0.8, 1.1), 'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms', 'resolution_m': 60},
        }
    }

    # shallow copy
    __map[SpacecraftID.LANDSAT_7] = copy.copy(__map[SpacecraftID.LANDSAT_45])
    __map[SpacecraftID.LANDSAT_7][Band.PANCHROMATIC] = {'number': 8, 'wavelength_range': (0.52, 0.90), 'description': '15 meter resolution, sharper image definition', 'resolution_m': 15}

    __enum_map = {}
    for spacecrafID in __map:
        for band_key in __map[spacecrafID]:
            if isinstance(__map[spacecrafID][band_key], dict):
                if spacecrafID not in __enum_map:
                    __enum_map[spacecrafID] = {}
                __enum_map[spacecrafID][__map[spacecrafID][band_key]['number']] = band_key

    def __init__(self, spacecraft_id: SpacecraftID):
        if spacecraft_id & SpacecraftID.LANDSAT_123_MSS:
            self.__spacecraft_id = SpacecraftID.LANDSAT_123_MSS
        elif spacecraft_id & SpacecraftID.LANDSAT_45_MSS:
            self.__spacecraft_id = SpacecraftID.LANDSAT_45_MSS
        elif spacecraft_id & SpacecraftID.LANDSAT_45:
            self.__spacecraft_id = SpacecraftID.LANDSAT_45
        elif spacecraft_id & SpacecraftID.LANDSAT_7:
            self.__spacecraft_id = SpacecraftID.LANDSAT_7
        elif spacecraft_id == SpacecraftID.LANDSAT_8:
            self.__spacecraft_id = SpacecraftID.LANDSAT_8
        else:
            self.__spacecraft_id = None

    def get_name(self, band_number):
        return self.__enum_map[self.__spacecraft_id][band_number].name

    def get_band_enum(self, band_number):
        return self.__enum_map[self.__spacecraft_id][band_number]

    def get_number(self, band_enum: Band):
        return self.__map[self.__spacecraft_id][band_enum]['number']

    def get_resolution(self, band_enum: Band):
        return self.__map[self.__spacecraft_id][band_enum]['resolution_m']

    def get_details(self):
        return self.__map[self.__spacecraft_id]

    def get_max_resolution(self):
        return self.__map[self.__spacecraft_id]['max_resolution']


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
        # TODO, this could use a shallow copy? instead of creating an object like this? And thne all the attributes would just call the array indices?
        self.scene_id = row[0]  # STRING	REQUIRED   Unique identifier for a particular Landsat image downlinked to a particular ground station.
        self.product_id = row[1]  # STRING	NULLABLE Unique identifier for a particular scene processed by the USGS at a particular time, or null for pre-collection data.
        self.spacecraft_id = SpacecraftID[row[2].upper()]  # SpacecraftID REQUIRED The spacecraft that acquired this scene: one of 'LANDSAT_4' through 'LANDSAT_8'.
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

        self.band_map = BandMap(self.spacecraft_id)
        self.center_lat = (self.north_lat - self.south_lat) / 2 + self.south_lat
        self.center_lon = (self.east_lon - self.west_lon) / 2 + self.west_lon

        self.utm_epsg_code = self.get_utm_epsg_code(self.center_lon, self.center_lat)

        #  (minx, miny, maxx, maxy)
        self.bounds = (self.west_lon, self.south_lat, self.east_lon, self.north_lat)

        gsurl = urlparse(self.base_url)
        self.bucket_name = gsurl[1]
        self.data_prefix = gsurl[2]
        self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + self.data_prefix.strip("\/")
        self.base_mount_path = base_mount_path

        # self.__file_list = None
        # self.thread = threading.Thread(target=self.__query_file_list(), args=())
        # self.thread.daemon = True
        # self.thread.start()
        self.__wrs_geometries = WRSGeometries()

    def get_wrs_polygon(self):
        return self.__wrs_geometries.get_wrs_geometry(self.wrs_path, self.wrs_row, timeout=60)

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

    def __query_file_list(self):
        bucket = self.__storage_client.list_buckets(prefix=self.bucket_name + self.data_prefix)
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
        self.data_id = None
        self.bounds = None
        if metadata:
            # TODO more elegant please
            name_prefix = metadata.product_id
            if not metadata.product_id:
                name_prefix = metadata.scene_id

            file_path = "{0}/{1}_B{2}.TIF".format(metadata.full_mount_path, name_prefix, band_number)

            dataset = gdal.Open(file_path)

            self.data_type = gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType)

            self.x_src_size = dataset.RasterXSize
            self.y_src_size = dataset.RasterYSize
            self.x_dst_size = dataset.RasterXSize
            self.y_dst_size = dataset.RasterYSize

            self.projection = dataset.GetProjection()
            self.geo_transform = dataset.GetGeoTransform()
            self.data_id = name_prefix

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
    def __init__(self, bucket_name):
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

    def __calculate_metadata(self, metadata: Metadata, band_definitions: list, extent: tuple=None, extent_cs=None) -> RasterMetadata:
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
                            cutline_wkb: bytes=None,
                            extent: tuple=None,
                            extent_cs: pyproj.Proj=None,
                            output_type: DataType = DataType.BYTE) -> np.ndarray:
        # TODO remove this, right?
        if cutline_wkb:
            extent = shapely.wkb.loads(cutline_wkb).bounds

        dataset = self.get_dataset(band_definitions,
                                   output_type=output_type,
                                   scale_params=scale_params,
                                   extent=extent,
                                   cutline_wkb=cutline_wkb)
        nda = dataset.ReadAsArray()
        del dataset
        
        if len(band_definitions) > 2:
            return nda.transpose((1, 2, 0))
        return nda

    def __get_source_elem(self, band_number, calculated_metadata: RasterMetadata, block_size=256):
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

        if band_definition.transfer_type:
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

    def __get_band_elem(self, vrt_dataset, band_number, position_number, calculated_metadata: RasterMetadata, metadata, block_size=256):
        # I think this needs to be removed.
        color_interp = metadata.band_map.get_name(band_number).capitalize()

        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        if color_interp is not None:
            etree.SubElement(elem_raster_band, "ColorInterp").text = color_interp

        elem_simple_source = self.__get_source_elem(band_number, calculated_metadata, block_size)
        elem_raster_band.append(elem_simple_source)

        elem_raster_band.set("dataType", calculated_metadata.get_metadata(band_number).data_type)
        elem_raster_band.set("band", str(position_number))

    def get_vrt(self,
                band_definitions: list,
                metadata: Metadata=None,
                translate_args=None,
                extent: tuple=None,
                extent_cs: pyproj.Proj=None,
                xRes=30, yRes=30):
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
        calculated_metadata = self.__calculate_metadata(metadata, band_definitions, extent=extent, extent_cs=extent_cs)
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
                                  extent: tuple=None):
        translated = []
        for metadata in self.__metadata:
            if self.storage.mount_sub_folder(metadata, request_key=str(self.__id)) is False:
                return None

            vrt = self.get_vrt(band_definitions, metadata=metadata, extent=extent)
            # http://gdal.org/python/
            # http://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
            dataset_translated = gdal.Translate('', vrt.decode('utf-8'),
                                                format='MEM',
                                                scaleParams=scale_params,
                                                xRes=60, yRes=60,
                                                outputType=output_type.gdal,
                                                noData=0)
            translated.append(dataset_translated)
        return translated

    def get_dataset(self,
                    band_definitions,
                    output_type: DataType,
                    scale_params=None,
                    extent: tuple = None,
                    cutline_wkb: bytes = None):
        dataset_translated = self.__get_translated_datasets(band_definitions, output_type, scale_params, extent)

        b_alpha_channel = Band.ALPHA in band_definitions
        # if there is no need to warp the data
        if not cutline_wkb and len(dataset_translated) == 1 and not b_alpha_channel:
            return dataset_translated

        dataset_warped = self.__get_warped(dataset_translated,
                                           output_type=output_type,
                                           cutline_wkb=cutline_wkb,
                                           dstAlpha=b_alpha_channel)

        for dataset in dataset_translated:
            del dataset

        return dataset_warped

    def __get_warped(self, dataset_translated: ogr, output_type: DataType, cutline_wkb: bytes=None, dstAlpha: bool=False):
        cutlineDSName = None
        if cutline_wkb:
            cutlineDSName = '/vsimem/cutline.json'
            cutline_ds = ogr.GetDriverByName('GeoJSON').CreateDataSource(cutlineDSName)
            cutline_lyr = cutline_ds.CreateLayer('cutline')
            f = ogr.Feature(cutline_lyr.GetLayerDefn())

            f.SetGeometry(ogr.CreateGeometryFromWkb(cutline_wkb))
            cutline_lyr.CreateFeature(f)
            f = None
            cutline_lyr = None
            cutline_ds = None

        dataset_warped = gdal.Warp("", dataset_translated, format='MEM', multithread=True, cutlineDSName=cutlineDSName, outputType=output_type.gdal, dstAlpha=dstAlpha)
        return dataset_warped


class Sentinel2:
    bucket_name = ""


class MetadataService(metaclass=__Singleton):
    """
    Notes on WRS-2 Landsat 8's Operational Land Imager (OLI) and/or Thermal Infrared Sensor (TIRS) sensors acquired nearly 10,000 scenes from just after its February 11, 2013 launch through April 10, 2013, during when the satellite was moving into the operational WRS-2 orbit. The earliest images are TIRS data only.  While these data meet the quality standards and have the same geometric precision as data acquired on and after April 10, 2013, the geographic extents of each scene will differ. Many of the scenes are processed to full terrain correction, with a pixel size of 30 meters. There may be some differences in the spatial resolution of the early TIRS images due to telescope temperature changes.
    """

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

    def search(
            self,
            satellite_id=None,
            bounding_box=None,
            start_date=None,
            end_date=None,
            sort_by=None,
            limit=10,
            sql_filters=None):
        # # Perform a synchronous query.
        query_builder = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index]'

        clause_start = 'WHERE'
        if satellite_id:
            query_builder += ' {0} spacecraft_id="{1}"'.format(clause_start, satellite_id.name)
            clause_start = 'AND'

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
                query_builder += ' {2} (({0} <= west_lon AND {1} >= west_lon) OR ' \
                                 '({0} >= west_lon AND east_lon >= {0}))'.format(minx, maxx, clause_start)
                query_builder += ' AND ((south_lat <= {0} AND north_lat >= {0}) OR ' \
                                 '(south_lat > {0} AND {1} >= south_lat))'.format(miny, maxy)
            clause_start = 'AND'

        if start_date is not None:
            query_builder += ' {0} date_acquired>="{1}"'.format(clause_start, start_date.isoformat())
            clause_start = 'AND'
        if end_date is not None:
            query_builder += ' {0} date_acquired<="{1}"'.format(clause_start, end_date.isoformat())
            clause_start = 'AND'

        if sql_filters is not None and len(sql_filters) > 0:

            query_builder += ' {0} {1}'.format(clause_start, sql_filters[0])
            clause_start = 'AND'
            for idx in range(1, len(sql_filters)):
                query_builder += ' AND {}'.format(sql_filters[idx])

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


class Storage(metaclass=__Singleton):
    bucket = ""
    __mounted_sub_folders = None

    def __init__(self, bucket_name="gcp-public-data-landsat"):
        self.bucket = bucket_name
        self.__mounted_sub_folders = {}

    def __del__(self):
        for full_path in self.__mounted_sub_folders:
            self.__unmount_sub_folder(full_path, "", force=True)

    def is_mounted(self, metadata: Metadata):
        if metadata.full_mount_path in self.__mounted_sub_folders and \
                self.__mounted_sub_folders[metadata.full_mount_path]:
            return True
        return False

    def mount_sub_folder(self, metadata: Metadata, request_key="temp"):
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
Notes on WRS-2 Landsat 8's Operational Land Imager (OLI) and/or Thermal Infrared Sensor (TIRS) sensors acquired nearly 10,000 scenes from just after its February 11, 2013 launch through April 10, 2013, during when the satellite was moving into the operational WRS-2 orbit. The earliest images are TIRS data only.  While these data meet the quality standards and have the same geometric precision as data acquired on and after April 10, 2013, the geographic extents of each scene will differ. Many of the scenes are processed to full terrain correction, with a pixel size of 30 meters. There may be some differences in the spatial resolution of the early TIRS images due to telescope temperature changes.
    """
    def __init__(self):
        self.__wrs2_map = {}

        # do some async query to check if the danger_zone needs updating
        self.__read_thread = threading.Thread(target=self.__read_shapefiles, args=())
        self.__read_thread.daemon = True  # Daemonize thread
        self.__read_thread.start()

    def __read_shapefiles(self):
        # self.__wrs1 = shapefile.Reader("/.epl/metadata/wrs/wrs1_asc_desc/wrs1_asc_desc.shp")
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

            self.__wrs2_map[path_num][row_num] = wrs2.shape(idx).__geo_interface__

    def get_wrs_geometry(self, wrs_path, wrs_row, timeout=10):
        self.__read_thread.join(timeout=timeout)
        if self.__read_thread.is_alive():
            return None

        return self.__wrs2_map[wrs_path][wrs_row]
