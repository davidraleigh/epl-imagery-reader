import os
import errno
import sys

from osgeo import gdal
from urllib.parse import urlparse
from lxml import etree
from enum import Enum
from subprocess import call

# Imports the Google Cloud client library
from google.cloud import bigquery


class SpacecraftID(Enum):
    LANDSAT_1 = 1
    LANDSAT_2 = 2
    LANDSAT_3 = 3
    LANDSAT_4 = 4
    LANDSAT_5 = 5
    LANDSAT_7 = 7
    LANDSAT_8 = 8


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

    __band_names = None
    __band_numbers = None
    __band_set_1 = ["Blue", "Green", "Red", "NIR", "SWIR1"]
    __band_set_2 = ["SWIR2", "Panchromatic", "Cirrus", "TIRS1", "TIRS2"]

    def __init__(self, spacecraft_id):
        self.__band_names = {}
        self.__band_numbers = {}
        index = 1
        if spacecraft_id.value > 7:
            self.__band_names[index] = "UltraBlue"
            self.__band_numbers[self.__band_names[index]] = index
            index = 2

        for i in range(0, len(self.__band_set_1)):
            self.__band_names[index + i] = self.__band_set_1[i]
            self.__band_numbers[self.__band_names[index + i]] = index + i

        index = index + len(self.__band_set_1)
        if spacecraft_id.value < 8:
            self.__band_names[index] = "Thermal"
            self.__band_numbers[self.__band_names[index]] = index
            index += 1

        for i in range(0, len(self.__band_set_2)):
            if (spacecraft_id.value < 7 and i > 0) or (spacecraft_id.value < 8 and i > 1):
                break
            self.__band_names[index + i] = self.__band_set_2[i]
            self.__band_numbers[self.__band_names[index + i]] = index + i

    def get_band_name(self, band_number):
        return self.__band_names[band_number]

    def get_band_number(self, band_name):
        return self.__band_numbers[band_name]


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
    __band_map = None
    __metadata = None

    def __init__(self, metadata):
        bucket_name = "gcp-public-data-landsat"
        super().__init__(bucket_name)
        self.__band_map = BandMap(metadata.spacecraft_id)
        self.__metadata = metadata

    def fetch_imagery_array(self, band_numbers, scaleParams=None):
        if self.storage.mount_sub_folder(self.__metadata) is False:
            return None

        return self.__get_ndarray(band_numbers, scaleParams)

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

    def get_vrt(self, band_numbers, translate_args=None):
        vrt_dataset = etree.Element("VRTDataset")

        position_number = 1
        max_x = sys.float_info.min
        max_y = sys.float_info.min

        for band in band_numbers:
            # TODO more elegant please
            if not self.__metadata.product_id:
                file_path = self.__metadata.full_mount_path + os.path.sep + self.__metadata.scene_id + "_B{}.TIF".format(band)
            else:
                file_path = self.__metadata.full_mount_path + os.path.sep + self.__metadata.product_id + "_B{}.TIF".format(band)

            dataset = gdal.Open(file_path)
            # TODO, check that this matters. I think maybe it doesn't
            max_x = dataset.RasterXSize if dataset.RasterXSize > max_x else max_x
            max_y = dataset.RasterYSize if dataset.RasterYSize > max_y else max_y

            color_interp = self.__band_map.get_band_name(band)

            self.get_raster_band_elem(
                vrt_dataset,
                gdal.GetDataTypeName(dataset.GetRasterBand(1).DataType),
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

    def __get_ndarray(self, band_numbers, scaleParams=None):
        vrt = self.get_vrt(band_numbers)
        # http://gdal.org/python/
        # http://gdal.org/python/osgeo.gdal-module.html#TranslateOptions
        # vrt_projected = gdal.Translate('', vrt, of="VRT", scaleParams=[], ot="Byte")
        # assumes input is unsigned int and output it Bytes and resolution is 60 meters
        dataset = gdal.Translate('', str(vrt), format="VRT",
                                 scaleParams=scaleParams,
                                 xRes=60, yRes=60, outputType=gdal.GDT_Byte, noData=0)
        nda = dataset.ReadAsArray().transpose((1, 2, 0))
        return nda


class Sentinel2:
    bucket_name = ""


class Metadata:
    def __init__(self, row, base_mount_path=None):
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
        gsurl = urlparse(self.base_url)
        self.full_mount_path = base_mount_path.rstrip("\/") + os.path.sep + gsurl[2].strip("\/")
        self.base_mount_path = base_mount_path


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

    def search(
            self,
            satellite_id,
            bounding_box=None,
            start_date=None,
            end_date=None,
            sort_by=None,
            limit=10,
            sql_filters=None):
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
                query_builder += ' AND {}'.format(sql_filter)

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

    def mount_sub_folder(self, metadata):
        # execute mount command
        # gcsfuse --only-dir LC08/PRE/044/034/LC80440342016259LGN00 gcp-public-data-landsat /landsat

        # full_mount_path = base_path.rstrip("\/") + os.path.sep + bucket_sub_folder.strip("\/")
        # subprocess.run("exit 1", shell=True, check=True)
        # subprocess.run(["ls", "-l", "/dev/null"], stdout=subprocess.PIPE)
        if metadata.full_mount_path in self.mounted_sub_folders:
            return True

        try:
            if not os.path.isdir(metadata.full_mount_path):
                os.makedirs(metadata.full_mount_path)
            else:
                # check to see if directory is already mounted if so maybe just return True?
                if len(os.listdir(metadata.full_mount_path)) > 0:
                    return True
                # hard to know what to do if it's mounted and it's empty...
                # TODO make a test for that case

        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        val = call(["gcsfuse", "--only-dir", metadata.full_mount_path.lstrip(metadata.base_mount_path).lstrip("\/"), self.bucket, metadata.full_mount_path])
        # TODO return error message if necessary
        if val != 0:
            return False

        self.mounted_sub_folders[metadata.full_mount_path] = True
        return True



