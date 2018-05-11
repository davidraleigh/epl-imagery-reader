import os
import py_compile
import unittest
from datetime import date
from urllib.parse import urlparse

import numpy as np
import pyproj
import requests
import shapely.geometry
from lxml import etree
from osgeo import gdal
from shapely.wkt import loads

from epl.imagery import PLATFORM_PROVIDER
from epl.imagery.reader import MetadataService, Landsat, \
    Storage, SpacecraftID, Metadata, BandMap, Band, \
    RasterMetadata, DataType, FunctionDetails
from epl.imagery.metadata_helpers import LandsatQueryFilters
from test.tools.test_helpers import xml_compare


class TestGCPMetadataSQL(unittest.TestCase):
    def test_all_sat_data(self):
        metadata_service = MetadataService()
        landsat_filters = LandsatQueryFilters()
        landsat_filters.cloud_cover.set_value(0)
        # sql_filters = ['cloud_cover=0']
        d_start = date(2004, 6, 24)
        d_end = date(2008, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -114.31054687499999, 35.84029065139799)
        rows = metadata_service.search(
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)
        rows = list(rows)
        first_item = rows[0]
        self.assertEqual(len(rows), 10)

        rows = metadata_service.search(
            satellite_id=SpacecraftID.UNKNOWN_SPACECRAFT,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)
        other_item = rows[0]
        self.assertEqual(len(rows), 10)

        self.assertEqual(first_item.scene_id, other_item.scene_id)

    def test_no_bounding_box(self):
        d_start = date(2003, 4, 4)
        d_end = date(2003, 4, 7)

        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_path.set_value(125)
        landsat_filters.wrs_row.set_value(49)

        # sql_filters = ['wrs_row=49', 'wrs_path=125']
        metadata_service = MetadataService()
        rows = metadata_service.search(
            satellite_id=None,
            start_date=d_start,
            end_date=d_end,
            bounding_box=None,
            data_filters=landsat_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 3)

    def test_metatdata_file_list(self):
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        metadata_service = MetadataService()
        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 5)
        bounding_box = polygon.bounds
        # sql_filters = ['wrs_row=79']
        landsat_filters = LandsatQueryFilters()
        # landsat_filters.wrs_path.set_value(125)
        landsat_filters.wrs_row.set_value(79)
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)

        metadata = rows[0]
        self.assertEqual(len(metadata.get_file_list()), 0)

    @unittest.skip("not sure why I put this test in or when it last passed.")
    def test_get_file(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                            bounding_box=bounding_box, limit=1)
        rows = list(rows)
        metadata = rows[0]
        landsat = Landsat(metadata)
        self.assertIsNotNone(landsat)
        vrt = landsat.get_vrt([4, 3, 2])
        self.assertIsNotNone(vrt)
        dataset = landsat.get_dataset([4, 3, 2], DataType.UINT16)
        self.assertIsNotNone(dataset)
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'


class TestGCPLandsat(unittest.TestCase):
    base_mount_path = '/imagery'
    metadata_service = None
    metadata_set = []
    r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
    taos_geom = r.json()
    taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])

    def setUp(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        self.metadata_service = MetadataService()

        sql_filters = ['collection_number="PRE"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")

        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=self.taos_shape.bounds,
            limit=10,
            data_filters=landsat_filters)

        rows = list(rows)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        for row in rows:
            self.metadata_set.append(row)

    def test_gdal_info(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            start_date=d_start,
                                            end_date=d_end,
                                            bounding_box=bounding_box,
                                            limit=1)
        rows = list(rows)
        metadata = rows[0]
        storage = Storage(metadata.bucket_name)

        b_mounted = storage.mount_sub_folder(metadata, "generic")
        self.assertTrue(b_mounted)
        b_deleted = storage.unmount_sub_folder(metadata, "generic")
        self.assertTrue(b_deleted)

    def test_landsat5_vrt(self):
        # 5th Place: Lake Eyre Landsat 5 Acquired August 5, 2006
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 5)
        bounding_box = polygon.bounds
        sql_filters = ['wrs_row=79']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_row.set_value(79)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)

        self.assertEqual(len(rows), 1)

        # data structure that contains all fields from Google's Landsat BigQuery Database
        metadata = rows[0]

        # GDAL helper functions for generating VRT
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([3, 2, 1])

        with open('testlandsat5.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10})
            self.assertTrue(result, message)

    def test_australia(self):
        # 5th Place: Lake Eyre Landsat 5 Acquired August 5, 2006
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_row.set_value(79)
        # sql_filters = ['wrs_row=79']
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)

        self.assertEqual(len(rows), 1)

        metadata = rows[0]
        landsat = Landsat(metadata)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [3, 2, 1]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        # nda = landsat.__get_ndarray(band_numbers, metadata, scale_params)
        nda = landsat.fetch_imagery_array(band_numbers, scale_params)
        self.assertEqual((3581, 4046, 3), nda.shape)

    def test_unmount_destructor(self):
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        # sql_filters = ['wrs_row=79']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_row.set_value(79)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)

        metadata = rows[0]
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([4])
        # storage = Storage("gcp-public-data-landsat")
        # del landsat
        # self.assertFalse(storage.is_mounted(metadata))

    def test_unmount_destructor_conflict(self):
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        # sql_filters = ['wrs_row=79']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_row.set_value(79)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)

        rows = list(rows)

        metadata = rows[0]
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([4])
        storage = Storage("gcp-public-data-landsat")
        landsat_2 = Landsat(metadata)
        vrt = landsat_2.get_vrt([4])
        del landsat
        self.assertTrue(storage.is_mounted(metadata))

    def test_vrt(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['scene_id="LC80400312016103LGN00"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80400312016103LGN00")
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                            bounding_box=bounding_box,
                                            limit=1, data_filters=landsat_filters)

        rows = list(rows)

        metadata = rows[0]
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([4, 3, 2])
        with open('test_1.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual)
            self.assertTrue(result, message)

        dataset = gdal.Open(vrt)
        self.assertIsNotNone(dataset)

        ds_band_1 = dataset.GetRasterBand(1)
        self.assertIsNotNone(ds_band_1)
        self.assertEqual(ds_band_1.XSize, 7631)
        ds_band_2 = dataset.GetRasterBand(2)
        self.assertIsNotNone(ds_band_2)
        self.assertEqual(ds_band_2.YSize, 7771)
        ds_band_3 = dataset.GetRasterBand(3)
        self.assertIsNotNone(ds_band_3)
        self.assertEqual(ds_band_3.YSize, 7771)


class TestStorage(unittest.TestCase):
    base_mount_path = '/imagery'



    def test_storage_create(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)

        rows = list(rows)

        metadata = rows[0]
        storage = Storage(metadata.bucket_name)

        metadata = rows[0]
        self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))

    def test_singleton(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box, limit=1)

        rows = list(rows)

        metadata = rows[0]
        storage_1 = Storage(metadata.bucket_name)
        storage_2 = Storage(metadata.bucket_name)
        self.assertTrue(storage_1 is storage_2)

    def test_delete_storage(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box, limit=1)
        rows = list(rows)
        metadata = rows[0]
        # storage = Storage(metadata.bucket_name)
        #
        # # self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        # files = [f for f in os.listdir(metadata.full_mount_path) if
        #          os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        # self.assertTrue(len(files) > 0)
        # # self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))
        # files = [f for f in os.listdir(metadata.full_mount_path) if
        #          os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        # self.assertEqual(len(files), 0)
        # # self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        # files = [f for f in os.listdir(metadata.full_mount_path) if
        #          os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        # self.assertTrue(len(files) > 0)
        # self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))

    def test_platform_provider(self):
        self.assertEqual("GCP", PLATFORM_PROVIDER)


class TestGCPPixelFunctions(unittest.TestCase):
    m_metadata = None
    base_mount_path = '/imagery'
    metadata_service = MetadataService()
    iowa_polygon = None
    metadata_set = []
    r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
    taos_geom = r.json()
    taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])

    def setUp(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['scene_id="LC80400312016103LGN00"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80400312016103LGN00")
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box,
                                       limit=1, data_filters=landsat_filters)
        rows = list(rows)
        self.m_metadata = rows[0]
        wkt_iowa = "POLYGON((-93.76075744628906 42.32707774458643,-93.47854614257812 42.32707774458643," \
                   "-93.47854614257812 42.12674735753131,-93.76075744628906 42.12674735753131," \
                   "-93.76075744628906 42.32707774458643))"
        self.iowa_polygon = loads(wkt_iowa)
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")

        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        sql_filters = ['collection_number="PRE"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=self.taos_shape.bounds,
            limit=10,
            data_filters=landsat_filters)

        rows = list(rows)

        for row in rows:
            self.metadata_set.append(row)

    def test_pixel_1(self):
        metadata = self.m_metadata
        landsat = Landsat(metadata)  # , gsurl[2])

        code = """import numpy as np
def multiply_rounded(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,
                   raster_ysize, buf_radius, gt, **kwargs):
    factor = float(kwargs['factor'])
    out_ar[:] = np.round_(np.clip(in_ar[0] * factor,0,255))"""

        function_arguments = {"factor": "1.5"}
        pixel_function_details = FunctionDetails(name="multiply_rounded", band_definitions=[2],
                                                 data_type=DataType.FLOAT32, code=code,
                                                 arguments=function_arguments)

        vrt = landsat.get_vrt([pixel_function_details, 3, 2])

        with open('pixel_1.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10})
            self.assertTrue(result, message)


    def test_pixel_ndvi(self):
        """
        http://grindgis.com/blog/vegetation-indices-arcgis
        NDVI = (NIR - RED) / (NIR + RED)
        NDVI = (5 - 4) / (5 + 4)
        :return:
        """
        landsat = Landsat(self.m_metadata)  # , gsurl[2])

        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        output = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
        output[np.isnan(output)] = 0.0
        # shift range from -1.0-1.0 to 0.0-2.0
        output += 1.0
        # scale up from 0.0-2.0 to 0 to 255 by multiplying by 255/2
        # https://stackoverflow.com/a/1735122/445372
        output *=  65535/2.0
        # https://stackoverflow.com/a/10622758/445372
        # in place type conversion
        out_ar[:] = output.astype(np.int16, copy=False)"""

        pixel_function_details = FunctionDetails(name="ndvi_numpy", band_definitions=[4, 5],
                                                 data_type=DataType.UINT16, code=code)
        vrt = landsat.get_vrt([pixel_function_details, 3, 2])

        with open('ndvi_numpy.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10})
            self.assertTrue(result, message)

        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")

        ds = gdal.Open(vrt)
        self.assertIsNotNone(ds)
        arr_ndvi = ds.GetRasterBand(1).ReadAsArray()
        ds = None
        self.assertIsNotNone(arr_ndvi)
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        band_definitions = [pixel_function_details, 3, 2]
        nda = landsat.fetch_imagery_array(band_definitions, scale_params)
        self.assertIsNotNone(nda)


    @staticmethod
    def ndvi_numpy(nir, red):
        with np.errstate(divide='ignore', invalid='ignore'):
            out_ar = np.divide((nir.astype(float) - red.astype(float)), (nir.astype(float) + red.astype(float)))
            out_ar[np.isnan(out_ar)] = 0.0
            return out_ar

    def test_iowa_ndarray(self):
        d_start = date(2016, 4, 4)
        d_end = date(2016, 8, 7)
        bounding_box = self.iowa_polygon.bounds
        # sql_filters = ["cloud_cover<=15"]
        landsat_filters = LandsatQueryFilters()
        landsat_filters.cloud_cover.set_range_end(15, True)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)
        rows = list(rows)
        metadata = rows[0]
        landsat = Landsat(metadata)

        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        out_ar[:] = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
        out_ar[np.isnan(out_ar)] = 0.0
        out_ar """

        pixel_function_details = FunctionDetails(name="ndvi_numpy", band_definitions=[4, 5], code=code, data_type=DataType.FLOAT32)
        # pixel_function_details = {
        #     "band_numbers": [4, 5],
        #     "function_code": code,
        #     "function_type": "ndvi_numpy",
        #     "data_type": DataType.FLOAT32,
        # }

        band_definitions = [pixel_function_details, 4, 5]

        vrt = landsat.get_vrt(band_definitions)
        ds = gdal.Open(vrt)
        self.assertIsNotNone(ds)

        arr_4 = ds.GetRasterBand(2).ReadAsArray()
        arr_5 = ds.GetRasterBand(3).ReadAsArray()
        arr_ndvi = ds.GetRasterBand(1).ReadAsArray()
        del ds
        del landsat
        print(np.ndarray.max(arr_ndvi))
        print(np.ndarray.min(arr_ndvi))
        self.assertFalse(np.any(np.isinf(arr_ndvi)))
        self.assertIsNotNone(arr_ndvi)

        local_ndvi = self.ndvi_numpy(arr_5, arr_4)

        del arr_4
        del arr_5
        self.assertFalse(np.any(np.isinf(local_ndvi)))

        np.testing.assert_almost_equal(arr_ndvi, local_ndvi)

    def test_iowa_scaled(self):
        d_start = date(2016, 4, 4)
        d_end = date(2016, 8, 7)
        bounding_box = self.iowa_polygon.bounds
        # sql_filters = ["cloud_cover<=15"]
        landsat_filters = LandsatQueryFilters()
        landsat_filters.cloud_cover.set_range_end(15, True)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            data_filters=landsat_filters)
        rows = list(rows)
        metadata = rows[0]
        landsat = Landsat(metadata)

        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        factor = float(kwargs['factor'])
        out_ar[:] = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
        out_ar[np.isnan(out_ar)] = 0.0
        # shift range from -1.0-1.0 to 0.0-2.0
        out_ar += 1.0
        # scale up from 0.0-2.0 to 0 to 255 by multiplying by 255/2
        out_ar *= factor/2.0"""

        # pixel_function_details = {
        #     "function_arguments": {"factor": 255},
        #     "band_numbers": [4, Band.NIR],
        #     "function_code": code,
        #     "function_type": "ndvi_numpy",
        #     "data_type": DataType.FLOAT32,
        # }

        pixel_function_details = FunctionDetails(name="ndvi_numpy",
                                                 band_definitions=[4, Band.NIR],
                                                 code=code, arguments={"factor": 255},
                                                 data_type=DataType.FLOAT32)

        band_definitions = [pixel_function_details, Band.RED, 5]

        vrt = landsat.get_vrt(band_definitions)
        ds = gdal.Open(vrt)

        self.assertIsNotNone(ds)

        arr_4 = ds.GetRasterBand(2).ReadAsArray()
        arr_5 = ds.GetRasterBand(3).ReadAsArray()
        arr_ndvi = ds.GetRasterBand(1).ReadAsArray()
        del ds
        del landsat
        print(np.ndarray.max(arr_ndvi))
        # print(np.ndarray.min(arr_ndvi))
        # self.assertFalse(np.any(np.isinf(arr_ndvi)))
        self.assertIsNotNone(arr_ndvi)

        local_ndvi = self.ndvi_numpy(arr_5, arr_4)
        del arr_4
        del arr_5

        local_ndvi += 1.0
        local_ndvi *= float(pixel_function_details.arguments['factor']) / 2.0
        self.assertFalse(np.any(np.isinf(local_ndvi)))

        np.floor(arr_ndvi, out=arr_ndvi)
        np.floor(local_ndvi, out=local_ndvi)
        np.testing.assert_almost_equal(arr_ndvi, local_ndvi, decimal=0)

    def test_malformed_funciton(self):
        code = """import numpy as np
        def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
        with np.errstate(divide = 'ignore', invalid = 'ignore'):
            factor = float(kwargs['factor'])
            out_ar[:] = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
            out_ar[np.isnan(out_ar)] = 0.0
            # shift range from -1.0-1.0 to 0.0-2.0
            out_ar += 1.0
            # scale up from 0.0-2.0 to 0 to 255 by multiplying by 255/2
            out_ar *= factor/2.0 """

        # pixel_function_details = {
        #     "function_arguments": {"factor": 255},
        #     "band_numbers": [4, 5],
        #     "function_code": code,
        #     "function_type": "ndvi_numpy",
        #     "data_type": DataType.FLOAT32,
        # }

        self.assertRaises(py_compile.PyCompileError, lambda: FunctionDetails(name="ndvi_numpy",
                                                                             code=code,
                                                                             band_definitions=[4, 5],
                                                                             data_type=DataType.FLOAT32,
                                                                             arguments={"factor": 255}))


    # def test_translate_vrt(self):
    #     #                                                          LC80390332016208LGN00
    """
    gdalbuildvrt -vrtnodata 0 0 0 -separate rgb_35.vrt /imagery/gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00/LC80330352017072LGN00_B4.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00/LC80330352017072LGN00_B3.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00/LC80330352017072LGN00_B2.TIF
    gdalbuildvrt -separate rgb_34.vrt /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B4.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B3.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B2.TIF
    """
    #     # gdal_translate -of VRT -ot Byte -scale -tr 60 60 rgb.vrt rgb_byte_scaled.vrt
    #
    #     self.assertTrue(True)
    #     sql_filters = ['scene_id="LC80330342017072LGN00"']
    #     metadata_service = MetadataService()
    #     rows = metadata_service.search(SpacecraftID.LANDSAT_8, data_filters=landsat_filters)
    #rows = list(rows)
    #
    #
    #     metadata = rows[0]
    #     gsurl = urlparse(metadata.base_url)
    #     storage = Storage(gsurl[1])
    #
    #     b_mounted = storage.mount_sub_folder(gsurl[2], self.base_mount_path)
    #     landsat = Landsat(base_mount_path, gsurl[2])
    #     vrt = landsat.get_vrt(metadata, [5, 4, 3])
    #
    #     with open('gdalbuildvrt_LC80390332016208LGN00.vrt', 'r') as myfile:
    #         data = myfile.read()
    #         expected = etree.XML(data)
    #         actual = etree.XML(vrt)
    #         result, message = xml_compare(expected, actual)
    #         self.assertTrue(result, message)

    def test_ndvi_taos(self):

        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        factor = float(kwargs['factor'])
        output = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
        output[np.isnan(output)] = 0.0
        # shift range from -1.0-1.0 to 0.0-2.0
        output += 1.0
        # scale up from 0.0-2.0 to 0 to 255 by multiplying by 255/2
        # https://stackoverflow.com/a/1735122/445372
        output *=  factor/2.0
        # https://stackoverflow.com/a/10622758/445372
        # in place type conversion
        out_ar[:] = output.astype(np.int16, copy=False)"""

        code2 = """import numpy as np
def ndvi_numpy2(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        output = (in_ar[1] - in_ar[0]) / (in_ar[1] + in_ar[0])
        output[np.isnan(output)] = 0.0
        out_ar[:] = output"""

        landsat = Landsat(self.metadata_set)

        scale_params = [[0, DataType.UINT16.range_max, -1.0, 1.0]]

        pixel_function_details = FunctionDetails(name="ndvi_numpy",
                                                 band_definitions=[Band.RED, Band.NIR],
                                                 code=code,
                                                 arguments={"factor": DataType.UINT16.range_max},
                                                 data_type=DataType.UINT16)

        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")
        nda = landsat.fetch_imagery_array([pixel_function_details],
                                          scale_params=scale_params,
                                          polygon_boundary_wkb=self.taos_shape.wkb,
                                          output_type=DataType.FLOAT32)

        self.assertIsNotNone(nda)
        self.assertGreaterEqual(1.0, nda.max())
        self.assertLessEqual(-1.0, nda.min())

        pixel_function_details = FunctionDetails(name="ndvi_numpy2",
                                                 band_definitions=[Band.RED, Band.NIR],
                                                 code=code2,
                                                 data_type=DataType.FLOAT32)

        nda2 = landsat.fetch_imagery_array([pixel_function_details],
                                           polygon_boundary_wkb=self.taos_shape.wkb,
                                           output_type=DataType.FLOAT32)

        self.assertIsNotNone(nda2)
        self.assertGreaterEqual(1.0, nda2.max())
        self.assertLessEqual(-1.0, nda2.min())

    def test_fail_1_to_1(self):
        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    out_ar[:] = in_ar[0]"""

        landsat = Landsat(self.metadata_set)
        scale_params = [[0, 40000], [0, 40000], [0, 40000]]

        pixel_function_details = FunctionDetails(name="ndvi_numpy",
                                                 band_definitions=[Band.RED],
                                                 code=code,
                                                 arguments={"factor": DataType.UINT16.range_max},
                                                 data_type=DataType.UINT16)

        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")
        nda = landsat.fetch_imagery_array([pixel_function_details, Band.GREEN, Band.BLUE],
                                          scale_params=scale_params,
                                          polygon_boundary_wkb=self.taos_shape.wkb,
                                          output_type=DataType.BYTE)

        nda2 = landsat.fetch_imagery_array([Band.RED, Band.GREEN, Band.BLUE],
                                           scale_params=scale_params,
                                           polygon_boundary_wkb=self.taos_shape.wkb,
                                           output_type=DataType.BYTE)
        self.assertIsNotNone(nda)
        np.testing.assert_almost_equal(nda, nda2)
        np.testing.assert_equal(nda, nda2)

    @unittest.skip("failing for some reason. unknown.")
    def test_native_vs_custom(self):
        landsat = Landsat(self.metadata_set)
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")
        pixel_native = FunctionDetails(name="sqrt",
                                       band_definitions=[Band.RED],
                                       data_type=DataType.UINT16,
                                       transfer_type=DataType.FLOAT32)
        nda = landsat.fetch_imagery_array([pixel_native],
                                          polygon_boundary_wkb=self.taos_shape.wkb,
                                          output_type=DataType.FLOAT32)

        self.assertIsNotNone(nda)

        # TODO add own sqrt function here


class TestRasterMetadata(unittest.TestCase):
    base_mount_path = '/imagery'
    metadata_service = None

    def setUp(self):
        self.metadata_service = MetadataService()

    def test_add_metadata_error(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['data_type="L1T"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.data_type.set_value("L1T")
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            start_date=d_start,
                                            end_date=d_end,
                                            bounding_box=bounding_box,
                                            limit=2,
                                            data_filters=landsat_filters)

        rows = list(rows)

        metadata_1 = rows[0]
        metadata_2 = rows[1]

        bands = [Band.RED, Band.BLUE, Band.GREEN]

        band_map = BandMap(SpacecraftID.LANDSAT_8)

        raster_metadata = RasterMetadata()

        storage = Storage()
        storage.mount_sub_folder(metadata_1)
        storage.mount_sub_folder(metadata_2)

        second = False
        for band in bands:
            band_number = band_map.get_number(band)

            if second:
                self.assertRaises(Exception, lambda: raster_metadata.add_metadata(band_number, metadata_2))
            raster_metadata.add_metadata(band_number, metadata_1)
            second = True

    # @unittest.skip("changed how bounds are queried")
    def test_bounds(self):
        metadata_service = MetadataService()
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80330342017072LGN00")
        landsat_filters.collection_number.set_value("PRE")
        # sql_filters = ['scene_id="LC80330342017072LGN00"', 'collection_number="PRE"']
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            data_filters=landsat_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 1)

        metadata = rows[0]

        bands = [Band.RED, Band.BLUE, Band.GREEN]

        band_map = BandMap(SpacecraftID.LANDSAT_8)

        raster_metadata = RasterMetadata()

        storage = Storage()
        storage.mount_sub_folder(metadata)

        for band in bands:
            band_number = band_map.get_number(band)
            raster_metadata.add_metadata(band_number, metadata)

        boundary = raster_metadata.bounds
        self.assertIsNotNone(boundary)

        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
        taos_geom = r.json()
        taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])
        clipped_raster = raster_metadata.calculate_clipped(taos_shape.bounds, pyproj.Proj(init='epsg:4326'))
        self.assertIsNotNone(clipped_raster.bounds)
        big_box = shapely.geometry.box(*boundary)
        small_box = shapely.geometry.box(*clipped_raster.bounds)
        self.assertTrue(big_box.contains(small_box))

    def test_metadata_extent(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
        taos_geom = r.json()
        print(taos_geom)

        taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])

        metadata_service = MetadataService()
        # sql_filters = ['scene_id="LC80330342017072LGN00"', 'collection_number="PRE"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80330342017072LGN00")
        landsat_filters.collection_number.set_value("PRE")
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            data_filters=landsat_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 1)

        metadata = rows[0]

        # GDAL helper functions for generating VRT
        landsat = Landsat(metadata)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        vrt = landsat.get_vrt(band_numbers, envelope_boundary=taos_shape.bounds)

        with open('clipped_LC80330342017072LGN00.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10, "xOff": 1e-10, "yOff": 1e-10})
            self.assertTrue(result, message)

        dataset = gdal.Open(vrt)
        geo_transform = dataset.GetGeoTransform()

        # self.assertEqual(geo_transform, raster_metadata.get_geotransform(taos_shape.bounds))
        # self.assertNotEqual(geo_transform, raster_metadata.get_geotransform())

        """
        gdal command for creating test data--/Users/davidraleigh/code/echopark/gcp-landsat-reader/test/clipped_LC80330342017072LGN00.vrt
        
        gdalbuildvrt -te 404696.67322238116 4028985.0 482408.22401454527 4094313.7809402538 -separate rgb_clipped.vrt /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B4.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B3.TIF /imagery/gcp-public-data-landsat/LC08/PRE/033/034/LC80330342017072LGN00/LC80330342017072LGN00_B2.TIF
        
        
        gdal command for creating test data--
        gdal_translate -ot Byte -tr 60 60 -of VRT -scale 0 65535 0 255 
        /opt/src/gcp-imagery-reader/rgb_clipped.vrt 
        /opt/src/gcp-imagery-reader/rgb_clipped_translated.vrt
        
        """

        # TODO test band values for SrcRect

        # TODO test vs. something that autatically clips by extent and exports to vrt

        # TODO test by getting extent of vrt, projecting back to wgs 84 and making sure it is contained by taos_geom
