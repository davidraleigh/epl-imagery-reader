import os
import errno
import sys
import threading

import tempfile
import py_compile

import numpy as np

from osgeo import gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum
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


class SpacecraftID(Enum):
    LANDSAT_1 = 1
    LANDSAT_2 = 2
    LANDSAT_3 = 3
    LANDSAT_4 = 4
    LANDSAT_5 = 5
    LANDSAT_7 = 7
    LANDSAT_8 = 8


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


class BandMap:
    """
    Landsat 4-5
    Band 1 - Blue	0.45-0.52	30
    Band 2 - Green	0.52-0.60	30
    Band 3 - Red	0.63-0.69	30
    Band 4 - Near Infrared (NIR)	0.76-0.90	30
    Band 5  - Shortwave Infrared (SWIR) 1	1.55-1.75	30
    Band 6 - Thermal	10.40-12.50	120* (30)
    Band 7 - Shortwave Infrared (SWIR) 2	2.08-2.35	30
    """

    """
    Landsat 7
    Band 1 - Blue	0.45-0.52	30
    Band 2 - Green	0.52-0.60	30
    Band 3 - Red	0.63-0.69	30
    Band 4 - Near Infrared (NIR)	0.77-0.90	30
    Band 5 - Shortwave Infrared (SWIR) 1	1.55-1.75	30
    Band 6 - Thermal	10.40-12.50	60 * (30)
    Band 7 - Shortwave Infrared (SWIR) 2	2.09-2.35	30
    Band 8 - Panchromatic	.52-.90	15
    """

    """
    Landsat 8
    Band 1 - Ultra Blue (coastal/aerosol)	0.435 - 0.451	30
    Band 2 - Blue	0.452 - 0.512	30
    Band 3 - Green	0.533 - 0.590	30
    Band 4 - Red	0.636 - 0.673	30
    Band 5 - Near Infrared (NIR)	0.851 - 0.879	30
    Band 6 - Shortwave Infrared (SWIR) 1	1.566 - 1.651	30
    Band 7 - Shortwave Infrared (SWIR) 2	2.107 - 2.294	30
    Band 8 - Panchromatic	0.503 - 0.676	15
    Band 9 - Cirrus	1.363 - 1.384	30
    Band 10 - Thermal Infrared (TIRS) 1	10.60 - 11.19	100 * (30)
    Band 11 - Thermal Infrared (TIRS) 2	11.50 - 12.51	100 * (30)
    """

    __band_enums = None
    __band_numbers = None
    __band_set_1 = [Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]
    __band_set_2 = [Band.SWIR2, Band.PANCHROMATIC, Band.CIRRUS, Band.TIRS1, Band.TIRS2]

    def __init__(self, spacecraft_id):
        self.__band_enums = {}
        self.__band_numbers = {}
        index = 1
        if spacecraft_id.value > 7:
            self.__band_enums[index] = Band.ULTRA_BLUE
            self.__band_numbers[self.__band_enums[index]] = index
            index = 2

        for i in range(0, len(self.__band_set_1)):
            self.__band_enums[index + i] = self.__band_set_1[i]
            self.__band_numbers[self.__band_enums[index + i]] = index + i

        index = index + len(self.__band_set_1)
        if spacecraft_id.value < 8:
            self.__band_enums[index] = Band.THERMAL
            self.__band_numbers[self.__band_enums[index]] = index
            index += 1

        for i in range(0, len(self.__band_set_2)):
            if (spacecraft_id.value < 7 and i > 0) or (spacecraft_id.value < 8 and i > 1):
                break
            self.__band_enums[index + i] = self.__band_set_2[i]
            self.__band_numbers[self.__band_enums[index + i]] = index + i

    def get_band_name(self, band_number):
        return self.__band_enums[band_number].name

    def get_band_enum(self, band_number):
        return self.__band_enums[band_number]

    def get_band_number(self, band_enum):
        return self.__band_numbers[band_enum]


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

    def __init__(self, metadata):
        bucket_name = "gcp-public-data-landsat"
        super().__init__(bucket_name)
        self.band_map = BandMap(metadata.spacecraft_id)
        self.__metadata = metadata
        self.__id = id(self)

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
                band_number = self.band_map.get_band_number(band_number)

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
        color_interp = self.band_map.get_band_name(band_number).capitalize()

        elem_raster_band = etree.SubElement(vrt_dataset, "VRTRasterBand")

        if color_interp is not None:
            etree.SubElement(elem_raster_band, "ColorInterp").text = color_interp

        elem_simple_source, x_size, y_size, projection, geo_transform, data_type = self.get_source_elem(band_number)
        elem_raster_band.append(elem_simple_source)

        elem_raster_band.set("dataType", data_type)
        elem_raster_band.set("band", str(position_number))

        return x_size, y_size, projection, geo_transform

    def get_vrt(self, band_definitions, translate_args=None):
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
                x_size, y_size, projection, geo_transform = self.get_band_elem(vrt_dataset, self.band_map.get_band_number(band_definition), position_number, 256)
            else:
                x_size, y_size, projection, geo_transform = self.get_band_elem(vrt_dataset, band_definition, position_number, 256)

            position_number += 1

            # TODO, check that this matters. I think maybe it doesn't
            max_x = x_size if x_size > max_x else max_x
            max_y = y_size if y_size > max_y else max_y

        vrt_dataset.set("rasterXSize", str(max_x))
        vrt_dataset.set("rasterYSize", str(max_y))
        etree.SubElement(vrt_dataset, "SRS").text = projection
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

        #  (minx, miny, maxx, maxy)
        self.bounds = (self.west_lon, self.south_lat, self.east_lon, self.north_lat)

        gsurl = urlparse(self.base_url)
        self.bucket_name = gsurl[1]
        self.data_prefix = gsurl[2]
        self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + self.data_prefix.strip("\/")
        self.base_mount_path = base_mount_path

        self.__file_list = None
        self.thread = threading.Thread(target=self.__query_file_list(), args=())
        self.thread.daemon = True
        self.thread.start()


        # thread = threading.Thread(target)

    def get_boundary_wkt(self):
        return "POLYGON (({0} {1}, {2} {1}, {2} {3}, {0} {3}, {0} {1}))".format(*self.bounds)

    def get_file_list(self, timeout=4):
        # 4 second timeout on info
        self.thread.join(timeout=timeout)
        # TODO if empty throw a warning?
        return self.__file_list

    def __query_file_list(self):
        bucket = self.__storage_client.list_buckets(prefix=self.bucket_name + self.data_prefix)
        results = []
        for i in bucket:
            results.append(i)
        self.__file_list = results
        # def __get_file_list(self):
        #     self.__file_list = None


class MetadataService(metaclass=__Singleton):
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

    def is_mounted(self, metadata):
        if metadata.full_mount_path in self.__mounted_sub_folders and \
                self.__mounted_sub_folders[metadata.full_mount_path]:
            return True
        return False

    def mount_sub_folder(self, metadata, request_key):
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
