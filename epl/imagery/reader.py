import os
import errno
import sys
import threading

import tempfile
import py_compile

import shapefile

import math
import pyproj
import copy

from pprint import pprint

from osgeo import gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum
from enum import IntEnum
from subprocess import call


# Imports the Google Cloud client library
from google.cloud import bigquery
from google.cloud import storage


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


class BandMap:
    __map = {
        SpacecraftID.LANDSAT_8: {
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
            Band.BLUE: {'number': 1, 'wavelength_range': (0.45, 0.52), 'description': 'Bathymetric mapping, distinguishing soil from vegetation, and deciduous from coniferous vegetation', 'resolution_m': 30},
            Band.GREEN: {'number': 2, 'wavelength_range': (0.52, 0.60), 'description': 'Emphasizes peak vegetation, which is useful for assessing plant vigor', 'resolution_m': 30},
            Band.RED: {'number': 3, 'wavelength_range': (0.63, 0.69), 'description': 'Discriminates vegetation slopes', 'resolution_m': 30},
            Band.NIR: {'number': 4, 'wavelength_range': (0.77, 0.90), 'description': 'Emphasizes biomass content and shorelines', 'resolution_m': 30},
            Band.SWIR1: {'number': 5, 'wavelength_range': (1.55, 1.75), 'description': 'Discriminates moisture content of soil and vegetation; penetrates thin clouds', 'resolution_m': 30},
            Band.THERMAL: {'number': 6, 'wavelength_range': (10.40, 12.50), 'description': 'Thermal mapping and estimated soil moisture (60m downsample Landsat7, 120m downsample landsat 4&5)', 'resolution_m': 30},
            Band.SWIR2: {'number': 7, 'wavelength_range': (2.09, 2.35), 'description': 'Hydrothermally altered rocks associated with mineral deposits', 'resolution_m': 30},
        },
        SpacecraftID.LANDSAT_123_MSS:{
            Band.GREEN: {'number': 4, 'wavelength_range': (0.5, 0.6), 'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 5, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features', 'resolution_m': 60},
            Band.INFRARED1: {'number': 6, 'wavelength_range': (0.7, 0.8), 'description': 'Vegetation boundary between land and water, and landforms', 'resolution_m': 60},
            Band.INFRARED2: {'number': 7, 'wavelength_range': (0.8, 1.1), 'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms', 'resolution_m': 60},
        },
        SpacecraftID.LANDSAT_45_MSS: {
            Band.GREEN: {'number': 1, 'wavelength_range': (0.5, 0.6), 'description': 'Sediment-laden water, delineates areas of shallow water', 'resolution_m': 60},
            Band.RED: {'number': 2, 'wavelength_range': (0.6, 0.7), 'description': 'Cultural features', 'resolution_m': 60},
            Band.INFRARED1: {'number': 3, 'wavelength_range': (0.7, 0.8), 'description': 'Vegetation boundary between land and water, and landforms', 'resolution_m': 60},
            Band.INFRARED2: {'number': 4, 'wavelength_range': (0.8, 1.1), 'description': 'Penetrates atmospheric haze best, emphasizes vegetation, boundary between land and water, and landforms', 'resolution_m': 60},
        }
    }

    # shallow copy
    __map[SpacecraftID.LANDSAT_7] = copy.copy(__map[SpacecraftID.LANDSAT_45])
    __map[SpacecraftID.LANDSAT_7][Band.PANCHROMATIC] = {'number': 8, 'wavelength_range': (0.52, 0.90), 'description': '15 meter resolution, sharper image definition', 'resolution_m': 15}


    # TODO this should all be turned into a singleton / Const value
    def __init__(self, spacecraft_id):
        self.__spacecraft_id = spacecraft_id
        self.__description_map = {}
        if spacecraft_id & SpacecraftID.LANDSAT_123_MSS:
            self.__description_map = self.__map[SpacecraftID.LANDSAT_123_MSS]
        elif spacecraft_id & SpacecraftID.LANDSAT_45_MSS:
            self.__description_map = self.__map[SpacecraftID.LANDSAT_45_MSS]
        elif spacecraft_id & SpacecraftID.LANDSAT_45:
            self.__description_map = self.__map[SpacecraftID.LANDSAT_45]
        elif spacecraft_id & SpacecraftID.LANDSAT_7:
            self.__description_map = self.__map[SpacecraftID.LANDSAT_7]
        elif spacecraft_id == SpacecraftID.LANDSAT_8:
            self.__description_map = self.__map[SpacecraftID.LANDSAT_8]
        else:
            self.__description_map = None

        __map_number = {}
        for key in self.__description_map:
            __map_number[self.__description_map[key]['number']] = key
        self.__enum_map = __map_number

    def get_name(self, band_number):
        return self.__enum_map[band_number].name

    def get_band_enum(self, band_number):
        return self.__enum_map[band_number]

    def get_number(self, band_enum):
        return self.__description_map[band_enum]['number']

    def get_resolution(self, band_enum):
        return self.__description_map[band_enum]['resolution_m']

    def get_details(self):
        return self.__description_map


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
    band_map = None
    __metadata = None
    __id = None

    def __init__(self, metadata: Metadata):
        bucket_name = "gcp-public-data-landsat"
        super().__init__(bucket_name)
        self.band_map = BandMap(metadata.spacecraft_id)
        self.__metadata = metadata
        self.__id = id(self)
        self.__wgs84_cs = pyproj.Proj(init='epsg:4326')

    def __del__(self):
        # log('\nbucket unmounted\n')
        self.storage.unmount_sub_folder(self.__metadata, request_key=self.__id)

    def fetch_imagery_array(self, band_definitions, scaleParams=None):
        # TODO move this under __init__? Maybe run it on a separate thread
        if self.storage.mount_sub_folder(self.__metadata, request_key=self.__id) is False:
            return None

        return self.__get_ndarray(band_definitions, scaleParams)

    def get_source_elem(self, band_number, block_size=256):
        elem_simple_source = etree.Element("SimpleSource")

        # if the input had multiple bands this setting would be where you change that
        # but the google landsat is one tif per band
        etree.SubElement(elem_simple_source, "SourceBand").text = str(1)

        elem_source_filename = etree.SubElement(elem_simple_source, "SourceFilename")
        elem_source_filename.set("relativeToVRT", "0")

        # TODO more elegant please
        name_prefix = self.__metadata.product_id
        if not self.__metadata.product_id:
            name_prefix = self.__metadata.scene_id

        file_path = "{0}/{1}_B{2}.TIF".format(self.__metadata.full_mount_path, name_prefix, band_number)
        elem_source_filename.text = file_path

        dataset = gdal.Open(file_path)

        data_type = gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType)

        x_size = dataset.RasterXSize
        y_size = dataset.RasterYSize
        projection = dataset.GetProjection()

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
        geo_transform = dataset.GetGeoTransform()

        del dataset

        elem_source_props = etree.SubElement(elem_simple_source, "SourceProperties")
        elem_source_props.set("RasterXSize", str(x_size))
        elem_source_props.set("RasterYSize", str(y_size))
        elem_source_props.set("DataType", data_type)

        # there may be a more efficient size than 256
        elem_source_props.set("BlockXSize", str(block_size))
        elem_source_props.set("BlockYSize", str(block_size))

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

        return elem_simple_source, x_size, y_size, projection, geo_transform, data_type

    def get_function_band_elem(self, vrt_dataset, band_definition, position_number, block_size):
        # data_type = gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType)
        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        elem_raster_band.set("dataType", band_definition['data_type'])
        elem_raster_band.set("band", str(position_number))
        elem_raster_band.set("subClass", "VRTDerivedRasterBand")

        # elem_simple_source = etree.SubElement(elem_raster_band, "SimpleSource")

        elem_function_language = etree.SubElement(elem_raster_band, "PixelFunctionLanguage")
        elem_function_language.text = "Python"

        elem_function_type = etree.SubElement(elem_raster_band, "PixelFunctionType")
        elem_function_type.text = band_definition["function_type"]

        if 'function_code' in band_definition:
            # TODO, still ugly that I have to use a temporary file: Also, stupid that I can't catch GDAL errors
            function_file = tempfile.NamedTemporaryFile(prefix=band_definition['function_type'], suffix=".py", delete=True)
            function_file.write(band_definition['function_code'].encode())
            function_file.flush()

            py_compile.compile(function_file.name, doraise=True)
            # delete file after compiling
            function_file.close()

            etree.SubElement(elem_raster_band, "PixelFunctionCode").text = band_definition["function_code"]

        if 'function_arguments' in band_definition:
            # <PixelFunctionArguments factor="1.5"/>
            elem_function_args = etree.SubElement(elem_raster_band, "PixelFunctionArguments")
            for function_arg_key in band_definition['function_arguments']:
                elem_function_args.set(function_arg_key, str(band_definition['function_arguments'][function_arg_key]))

        for band_number in band_definition["band_numbers"]:
            # TODO, I don't like this reuse of this variable
            if isinstance(band_number, Band):
                band_number = self.band_map.get_number(band_number)

            elem_simple_source, x_size, y_size, projection, geo_transform, data_type = self.get_source_elem(band_number)
            elem_raster_band.append(elem_simple_source)

        return x_size, y_size, projection, geo_transform

    def get_band_elem(self, vrt_dataset, band_number, position_number, block_size):

        # # TODO more elegant please
        # # TODO use get_source_elem
        # name_prefix = self.__metadata.product_id
        # if not self.__metadata.product_id:
        #     name_prefix = self.__metadata.scene_id
        #
        # file_path = "{0}/{1}_B{2}.TIF".format(self.__metadata.full_mount_path, name_prefix, band_number)
        # # file_path = self.__metadata.full_mount_path + os.path.sep + self.__metadata.scene_id + "_B{}.TIF".format(band)

        # I think this needs to be removed.
        color_interp = self.band_map.get_name(band_number).capitalize()

        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        if color_interp is not None:
            etree.SubElement(elem_raster_band, "ColorInterp").text = color_interp

        elem_simple_source, x_size, y_size, projection, geo_transform, data_type = self.get_source_elem(band_number)
        elem_raster_band.append(elem_simple_source)

        elem_raster_band.set("dataType", data_type)
        elem_raster_band.set("band", str(position_number))

        return x_size, y_size, projection, geo_transform

    def get_vrt(self, band_definitions: list, translate_args=None, extent: tuple=None, xRes=30, yRes=30):
        # TODO move this under __init__? Maybe run it on a separate thread
        if self.storage.mount_sub_folder(self.__metadata, request_key=self.__id) is False:
            return None

        vrt_dataset = etree.Element("VRTDataset")

        position_number = 1
        max_x = sys.float_info.min
        max_y = sys.float_info.min

        # TODO if no bands throw exception
        for band_definition in band_definitions:

            band_metadata = None
            if isinstance(band_definition, dict):
                x_size, y_size, projection, geo_transform = self.get_function_band_elem(vrt_dataset, band_definition, position_number, 256)
            elif isinstance(band_definition, Band):
                x_size, y_size, projection, geo_transform = self.get_band_elem(vrt_dataset, self.band_map.get_number(band_definition), position_number, 256)
            else:
                x_size, y_size, projection, geo_transform = self.get_band_elem(vrt_dataset, band_definition, position_number, 256)

            position_number += 1

            # TODO, check that this matters. I think maybe it doesn't
            max_x = x_size if x_size > max_x else max_x
            max_y = y_size if y_size > max_y else max_y

        vrt_dataset.set("rasterXSize", str(max_x))
        vrt_dataset.set("rasterYSize", str(max_y))
        etree.SubElement(vrt_dataset, "SRS").text = projection

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

        # TODO this is dangerous, just taking the epsg from the Metadata instead of from the raster. FIXME!!

        # proj_cs = pyproj.Proj(init='epsg:{0}'.format(self.__metadata.utm_epsg_code))
        # lon_ul_corner, lat_ul_corner = self.__wgs84_cs(self.__metadata.west_lon, self.__metadata.north_lat)
        # x_ul_corner, y_ul_corner = pyproj.transform(self.__wgs84_cs, proj_cs, lon_ul_corner, lat_ul_corner)
        # geo_transform = (x_ul_corner, xRes, 0, y_ul_corner, 0, -yRes)
        etree.SubElement(vrt_dataset, "GeoTransform").text = ",".join(map("  {:.16e}".format, geo_transform))

        return etree.tostring(vrt_dataset, encoding='UTF-8', method='xml')

    def __get_ndarray(self, band_definitions, scaleParams=None, additional_param=None):
        vrt = self.get_vrt(band_definitions)
        # http://gdal.org/python/
        # http://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
        # vrt_projected = gdal.Translate('', vrt, of="VRT", scaleParams=[], ot="Byte")
        # assumes input is unsigned int and output it Bytes and resolution is 60 meters
        # with tempfile.NamedTemporaryFile(vrt.decode("utf-8"), suffix=".vrt", delete=True) as temp_vrt:
        #     temp_vrt.write(vrt)
        #     temp_vrt.flush()
        dataset = gdal.Translate('', vrt.decode('utf-8'), format="VRT", scaleParams=scaleParams,
                                 xRes=60, yRes=60, outputType=gdal.GDT_Byte, noData=0)
        nda = dataset.ReadAsArray().transpose((1, 2, 0))
        return nda


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

    def __init__(self, bucket_name):
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

    def mount_sub_folder(self, metadata: Metadata, request_key):
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
