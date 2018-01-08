import unittest
import datetime

import requests
import shapely.geometry

import numpy as np

from osgeo import gdal
from urllib.parse import urlparse

from lxml import etree


from shapely.geometry import shape
from shapely.geometry import box
from shapely.wkt import loads

from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata, BandMap, Band, WRSGeometries, RasterBandMetadata, RasterMetadata, DataType, FunctionDetails


class TestMetaDataSQL(unittest.TestCase):
    def test_scene_id(self):
        sql_filters = ['scene_id="LC80390332016208LGN00"']
        metadata_service = MetadataService()
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, sql_filters=sql_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 1)

    def test_start_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertGreaterEqual(d_actual, d)

    def test_end_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_7, end_date=d)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_7.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d)

    def test_one_day(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d, end_date=d)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertEqual(d_actual, d)

    def test_1_year(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        metadata_rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                                start_date=d_start,
                                                end_date=d_end,
                                                bounding_box=bounding_box)

        metadata_rows = list(metadata_rows)

        self.assertEqual(len(metadata_rows), 10)
        for row in metadata_rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            test_box = row.bounds
            self.assertTrue(
                (bounding_box[0] < test_box[2] < bounding_box[2]) or
                (bounding_box[0] < test_box[0] < bounding_box[2]))
            self.assertTrue(
                (bounding_box[1] < test_box[3] < bounding_box[3]) or
                (bounding_box[1] < test_box[1] < bounding_box[3]))

    def test_cloud_cover(self):
        metadata_service = MetadataService()
        sql_filters = ['cloud_cover=0']
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        rows = list(rows)

        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            test_box = row.bounds
            self.assertTrue(
                (bounding_box[0] < test_box[2] < bounding_box[2]) or
                (bounding_box[0] < test_box[0] < bounding_box[2]))
            self.assertTrue(
                (bounding_box[1] < test_box[3] < bounding_box[3]) or
                (bounding_box[1] < test_box[1] < bounding_box[3]))

    def test_metadata_singleton(self):
        metadata_service_1 = MetadataService()
        metadata_service_2 = MetadataService()
        self.assertTrue(metadata_service_1 is metadata_service_2)


class TestMetadata(unittest.TestCase):
    def test_bounds(self):
        row = ('LC80330352017072LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2017-03-13', '2017-03-13T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        geom_obj = metadata.get_wrs_polygon()
        self.assertIsNotNone(geom_obj)
        bounding_polygon = box(*metadata.bounds)
        wrs_polygon = shape(geom_obj)
        self.assertTrue(bounding_polygon.contains(wrs_polygon))

        # polygon = loads(wkt)
        # self.assertEqual(polygon.wkt, wkt)
        # self.assertEqual(polygon.bounds, metadata.bounds)
        # self.assertTrue(True)

    def test_interesct(self):
        self.assertTrue(True)

    def test_epsg_codes(self):
        self.assertEqual(32601, Metadata.get_utm_epsg_code(-180, 45))
        self.assertEqual(32701, Metadata.get_utm_epsg_code(-180, -45))
        self.assertEqual(32601, Metadata.get_utm_epsg_code(-174, 45))
        self.assertEqual(32701, Metadata.get_utm_epsg_code(-174, -45))

        self.assertEqual(32602, Metadata.get_utm_epsg_code(-173.99, 45))
        self.assertEqual(32702, Metadata.get_utm_epsg_code(-173.99, -45))
        self.assertEqual(32602, Metadata.get_utm_epsg_code(-168, 45))
        self.assertEqual(32702, Metadata.get_utm_epsg_code(-168, -45))

        self.assertEqual(32603, Metadata.get_utm_epsg_code(-167.99, 45))
        self.assertEqual(32703, Metadata.get_utm_epsg_code(-167.99, -45))
        self.assertEqual(32603, Metadata.get_utm_epsg_code(-162, 45))
        self.assertEqual(32703, Metadata.get_utm_epsg_code(-162, -45))

        self.assertEqual(32660, Metadata.get_utm_epsg_code(180, 45))
        self.assertEqual(32760, Metadata.get_utm_epsg_code(180, -45))
        self.assertEqual(32660, Metadata.get_utm_epsg_code(174.00001, 45))
        self.assertEqual(32760, Metadata.get_utm_epsg_code(174.00001, -45))

        self.assertEqual(32659, Metadata.get_utm_epsg_code(174, 45))
        self.assertEqual(32759, Metadata.get_utm_epsg_code(174, -45))
        self.assertEqual(32659, Metadata.get_utm_epsg_code(168.00001, 45))
        self.assertEqual(32759, Metadata.get_utm_epsg_code(168.00001, -45))

    def test_utm_epsg(self):
        row = ('LC80330352017072LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2017-03-13', '2017-03-13T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        self.assertEqual(32613, metadata.utm_epsg_code)

    def test_doy(self):
        row = ('LC80330352017072LGN00_FAKED', '', 'LANDSAT_8', 'OLI_TIRS', '2017-11-04', '2017-11-04T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00_FAKE')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        self.assertEqual(308, metadata.doy)


class TestBandMap(unittest.TestCase):
    def test_landsat_5(self):
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertEqual(band_map.get_number(Band.BLUE), 1)
        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_number(Band.THERMAL), 6)

        self.assertEqual(band_map.get_band_enum(1), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)
        self.assertEqual(band_map.get_band_enum(6), Band.THERMAL)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 1), val)
            self.assertEqual(band_map.get_number(val), idx + 1)
            self.assertEqual(band_map.get_resolution(val), 30.0)

    def test_landsat_5_exceptions(self):
        band_map_2 = BandMap(SpacecraftID.LANDSAT_7)
        self.assertEqual(band_map_2.get_number(Band.PANCHROMATIC), 8)
        self.assertEqual(band_map_2.get_band_enum(8), Band.PANCHROMATIC)
        self.assertEqual(band_map_2.get_resolution(Band.PANCHROMATIC), 15.0)
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.CIRRUS))
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.PANCHROMATIC))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(8))

    def test_landsat_7(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertEqual(band_map.get_number(Band.BLUE), 1)
        self.assertEqual(band_map.get_number(Band.SWIR1), 5)
        self.assertEqual(band_map.get_number(Band.THERMAL), 6)

        self.assertEqual(band_map.get_number(Band.PANCHROMATIC), 8)
        self.assertEqual(band_map.get_band_enum(8), Band.PANCHROMATIC)
        self.assertEqual(band_map.get_resolution(Band.PANCHROMATIC), 15.0)

        self.assertEqual(band_map.get_band_enum(1), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(5), Band.SWIR1)
        self.assertEqual(band_map.get_band_enum(6), Band.THERMAL)
        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 1), val)
            self.assertEqual(band_map.get_number(val), idx + 1)
            self.assertEqual(band_map.get_resolution(val), 30.0)

    def test_landsat_7_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.CIRRUS))
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.TIRS1))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(9))

    def test_landsat_8(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertEqual(band_map.get_number(Band.ULTRA_BLUE), 1)
        self.assertEqual(band_map.get_number(Band.BLUE), 2)
        self.assertEqual(band_map.get_number(Band.SWIR1), 6)

        self.assertEqual(band_map.get_band_enum(2), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(6), Band.SWIR1)

        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 2), val)
            self.assertEqual(band_map.get_number(val), idx + 2)

        self.assertEqual(band_map.get_number(Band.CIRRUS), 9)
        self.assertEqual(band_map.get_number(Band.TIRS1), 10)
        self.assertEqual(band_map.get_number(Band.TIRS2), 11)
        self.assertEqual(band_map.get_resolution(Band.PANCHROMATIC), 15.0)

        self.assertEqual(band_map.get_band_enum(9), Band.CIRRUS)
        self.assertEqual(band_map.get_band_enum(10), Band.TIRS1)
        self.assertEqual(band_map.get_band_enum(11), Band.TIRS2)

    def test_landsat_8_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.THERMAL))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(12))


class TestWRSGeometries(unittest.TestCase):
    test_cases = [[15.74326, 26.98611, 1, 1, 1, 0, 13001, 13001, 13, 1, 'D', 1, 2233],
                  [2.74362, 6.65058, 942, 942, 1, 0, 61198, 61198, 61, 198, 'A', 1, 3174],
                  [13.37321, 24.20422, 2225, 2225, 1, 0, 125241, 125241, 125, 241, 'A', 2, 4209],
                  [3.58953, 7.7865, 1021, 1021, 1, 0, 75029, 75029, 75, 29, 'D', 3, 10445],
                  [4.2424, 8.69453, 891, 891, 1, 0, 64147, 64147, 64, 147, 'A', 6, 21227],
                  [16.81754, 27.20801, 3720, 3720, 1, 0, 223248, 223248, 223, 248, 'D', 16, 56296]]
    wrs_geometries = WRSGeometries()

    def test_geometry(self):
        for test_case in self.test_cases:
            geom_obj = self.wrs_geometries.get_wrs_geometry(test_case[8], test_case[9], timeout=60)
            geom_expected_area = test_case[0]

            self.assertIsNotNone(geom_obj)
            s = shape(geom_obj)
            self.assertAlmostEqual(geom_expected_area, s.area, 5)

    # def test_bounds_search(self):
    #     for idx, test_case in enumerate(self.test_cases):
    #         geom_obj = self.wrs_geometries.get_wrs_geometry(test_case[8], test_case[9], timeout=60)
    #         original_shape = shape(geom_obj)
    #         result = self.wrs_geometries.get_path_row(original_shape.bounds)
    #         path_pair = result.pop()
    #         while path_pair is not None:
    #             geom_obj = self.wrs_geometries.get_wrs_geometry(path_pair[0], path_pair[1])
    #             s = shape(geom_obj)
    #             b_intersect = s.envelope.intersects(original_shape.envelope)
    #             print("Test case {0}\n original bounds: {1}\nnon-intersecting bounds{2}\n".format(idx, original_shape.bounds, s.bounds))
    #             self.assertTrue(b_intersect, "Test case {0}\n original bounds: {1}\nnon-intersecting bounds{2}\n"
    #                             .format(idx, original_shape.bounds, s.bounds))
    #         break


class TestLandsat(unittest.TestCase):
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
        metadata_rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=self.taos_shape.bounds,
            limit=10,
            sql_filters=sql_filters)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        for row in metadata_rows:
            self.metadata_set.append(row)

    def test_gdal_info(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                            bounding_box=bounding_box,
                                            limit=1)
        rows = list(rows)
        metadata = rows[0]
        storage = Storage(metadata.bucket_name)

        b_mounted = storage.mount_sub_folder(metadata, "generic")
        self.assertTrue(b_mounted)
        b_deleted = storage.unmount_sub_folder(metadata, "generic")
        self.assertTrue(b_deleted)

    # TODO test PRE rejection
    # TODO test date range rejection
    # TODO test Satellite Rejection

    def test_vrt_not_pre(self):
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['collection_number!="PRE"']
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            start_date=d_start,
                                            end_date=d_end,
                                            bounding_box=bounding_box,
                                            limit=1,
                                            sql_filters=sql_filters)

        rows = list(rows)
        metadata = rows[0]

        landsat = Landsat(metadata)
        self.assertIsNotNone(landsat)
        vrt = landsat.get_vrt([4, 3, 2])
        self.assertIsNotNone(vrt)
        dataset = landsat.get_dataset([4, 3, 2], DataType.UINT16)
        self.assertIsNotNone(dataset)

    def test_pixel_function_vrt_1(self):
        utah_box = (-112.66342163085938, 37.738141282210385, -111.79824829101562, 38.44821130413263)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)

        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                            bounding_box=utah_box,
                                            limit=10, sql_filters=['collection_number=="PRE"', "cloud_cover<=5"])
        rows = list(rows)
        self.assertEqual(len(rows), 1)

        #     metadata_row = ['LC80390332016208LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2016-07-26',
        # '2016-07-26T18:14:46.9465460Z', 'PRE', 'N/A', 'L1T', 39, 33, 1.69,
        # 39.96962, 37.81744, -115.27267, -112.56732, 1070517542,
        # 'gs://gcp-public-data-landsat/LC08/PRE/039/033/LC80390332016208LGN00']
        metadata = rows[0]

        # GDAL helper functions for generating VRT
        landsat = Landsat([metadata])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [4, 3, 2]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params)

        self.assertEqual(nda.shape, (3861, 3786, 3))


        # src_ds = gdal.Open(input_file)
        # if src_ds is None:
        #     print
        #     'Unable to open %s' % input_file
        #     sys.exit(1)
        #
        # try:
        #     srcband = src_ds.GetRasterBand(band_num)
        # except RuntimeError, e:
        #     print
        #     'No band %i found' % band_num
        #     print
        #     e
        #     sys.exit(1)
        #
        # print
        # "[ NO DATA VALUE ] = ", srcband.GetNoDataValue()
        # print
        # "[ MIN ] = ", srcband.GetMinimum()
        # print
        # "[ MAX ] = ", srcband.GetMaximum()
        # print
        # "[ SCALE ] = ", srcband.GetScale()
        # print
        # "[ UNIT TYPE ] = ", srcband.GetUnitType()
        # ctable = srcband.GetColorTable()
        #
        # if ctable is None:
        #     print
        #     'No ColorTable found'
        #     sys.exit(1)
        #
        # print
        # "[ COLOR TABLE COUNT ] = ", ctable.GetCount()
        # for i in range(0, ctable.GetCount()):
        #     entry = ctable.GetColorEntry(i)
        #     if not entry:
        #         continue
        #     print
        #     "[ COLOR ENTRY RGB ] = ", ctable.GetColorEntryAsRGB(i, entry)

    # @unittest.skip("failing???")

    def test_band_enum(self):
        self.assertTrue(True)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, limit=1,
                                            sql_filters=['scene_id="LC80390332016208LGN00"'])
        rows = list(rows)
        metadata = rows[0]
        landsat = Landsat(metadata)
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        # nda = landsat.__get_ndarray(band_numbers, metadata, scale_params)
        nda = landsat.fetch_imagery_array([Band.RED, Band.GREEN, Band.BLUE], scale_params, xRes=240, yRes=240)
        self.assertIsNotNone(nda)
        nda2 = landsat.fetch_imagery_array([4, 3, 2], scale_params, xRes=240, yRes=240)
        np.testing.assert_almost_equal(nda, nda2)
        # 'scene_id': 'LC80390332016208LGN00'

    def test_vrt_extent(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set[0])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        vrt = landsat.get_vrt(band_numbers, extent=self.taos_shape.bounds)

        self.assertIsNotNone(vrt)

    def test_cutline(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set[0])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params, self.taos_shape.wkb, xRes=480, yRes=480)
        self.assertIsNotNone(nda)

        # TODO needs shape test

    def test_mosaic(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params, extent=self.taos_shape.bounds)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

        # TODO needs shape test

    def test_mosaic_cutline(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, cutline_wkb=self.taos_shape.wkb)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

    def test_mosaic_mem_error(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, extent=self.taos_shape.bounds)

        self.assertIsNotNone(nda)
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)
        self.assertEqual((1804, 1295, 3), nda.shape)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, cutline_wkb=self.taos_shape.wkb)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

    def test_datatypes(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]

        for data_type in DataType:
            nda = landsat.fetch_imagery_array(band_numbers, scaleParams, extent=self.taos_shape.bounds,
                                              output_type=data_type, yRes=240, xRes=240)
            self.assertIsNotNone(nda)
            self.assertGreaterEqual(data_type.range_max, nda.max())
            self.assertLessEqual(data_type.range_min, nda.min())

    def test_vrt_with_alpha(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE, Band.ALPHA]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]

        nda = landsat.fetch_imagery_array(band_numbers,
                                          scaleParams,
                                          extent=self.taos_shape.bounds,
                                          output_type=DataType.UINT16,
                                          xRes=120, yRes=120)
        self.assertIsNotNone(nda)

    def test_rastermetadata_cache(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, cutline_wkb=self.taos_shape.wkb, xRes=120, yRes=120)
        self.assertIsNotNone(nda)
        self.assertEqual((902, 648, 3), nda.shape)

        band_numbers = [Band.RED, Band.BLUE, Band.GREEN]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, cutline_wkb=self.taos_shape.wkb, xRes=120, yRes=120)
        self.assertIsNotNone(nda)
        self.assertEqual((902, 648, 3), nda.shape)

        band_numbers = [Band.RED, Band.BLUE, Band.GREEN]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, xRes=120, yRes=120)
        self.assertIsNotNone(nda)
        self.assertNotEqual((902, 648, 3), nda.shape)
