import os
import errno
import threading
import time

from lxml import etree
from enum import Enum
from subprocess import call
# Imports the Google Cloud client library
from google.cloud import bigquery


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

    def get_vrt(self, mounted_dir, band_numbers):
        
        vrt = """<VRTDataset rasterXSize="7711" rasterYSize="7851">
  <SRS>PROJCS["WGS 84 / UTM zone 13N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-105],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32613"]]</SRS>
  <GeoTransform>  4.4968500000000000e+05,  3.0000000000000000e+01,  0.0000000000000000e+00,  4.5822150000000000e+06,  0.0000000000000000e+00, -3.0000000000000000e+01</GeoTransform>
  <VRTRasterBand dataType="UInt16" band="1">
    <SimpleSource>
      <SourceFilename relativeToVRT="0">LC80330322015195LGN00_B5.TIF</SourceFilename>
      <SourceBand>1</SourceBand>
      <SourceProperties RasterXSize="7711" RasterYSize="7851" DataType="UInt16" BlockXSize="256" BlockYSize="256" />
      <SrcRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
      <DstRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
    </SimpleSource>
  </VRTRasterBand>
  <VRTRasterBand dataType="UInt16" band="2">
    <SimpleSource>
      <SourceFilename relativeToVRT="0">LC80330322015195LGN00_B4.TIF</SourceFilename>
      <SourceBand>1</SourceBand>
      <SourceProperties RasterXSize="7711" RasterYSize="7851" DataType="UInt16" BlockXSize="256" BlockYSize="256" />
      <SrcRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
      <DstRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
    </SimpleSource>
  </VRTRasterBand>
  <VRTRasterBand dataType="UInt16" band="3">
    <SimpleSource>
      <SourceFilename relativeToVRT="0">LC80330322015195LGN00_B3.TIF</SourceFilename>
      <SourceBand>1</SourceBand>
      <SourceProperties RasterXSize="7711" RasterYSize="7851" DataType="UInt16" BlockXSize="256" BlockYSize="256" />
      <SrcRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
      <DstRect xOff="0" yOff="0" xSize="7711" ySize="7851" />
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>"""
        return vrt

class Sentinel2:
    bucket_name = ""


class Metadata:
    def __init__(self, row):
        self.spacecraft_id = SpacecraftID.LANDSAT_8


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



