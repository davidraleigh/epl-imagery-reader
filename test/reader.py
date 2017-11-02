import unittest
import datetime

from shapely.wkt import loads
from shapely.geometry import shape
from shapely.geometry import box
from datetime import date
from epl.imagery.reader import MetadataService, Landsat,\
    Storage, SpacecraftID, Metadata, BandMap, Band, \
    WRSGeometries, RasterBandMetadata, RasterMetadata, DataType, FunctionDetails


class TestMetaDataSQL(unittest.TestCase):
    def test_scene_id(self):
        sql_filters = ['scene_id="LC80390332016208LGN00"']
        metadata_service = MetadataService()
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, sql_filters=sql_filters)
        self.assertEqual(len(rows), 1)

    def test_start_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertGreaterEqual(d_actual, d)

    def test_end_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_7, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_7.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d)

    def test_one_day(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertEqual(d_actual, d)

    def test_1_year(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            self.assertTrue((bounding_box[0] < row[14] < bounding_box[2]) or (bounding_box[0] < row[15] < bounding_box[2]))
            self.assertTrue((bounding_box[1] < row[12] < bounding_box[3]) or (bounding_box[1] < row[13] < bounding_box[3]))

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

        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            self.assertTrue(
                (bounding_box[0] < row[14] < bounding_box[2]) or (bounding_box[0] < row[15] < bounding_box[2]))
            self.assertTrue(
                (bounding_box[1] < row[12] < bounding_box[3]) or (bounding_box[1] < row[13] < bounding_box[3]))

    def test_no_bounding_box(self):
        d_start = date(2003, 4, 4)
        d_end = date(2003, 4, 7)
        sql_filters = ['wrs_row=49', 'wrs_path=125']
        metadata_service = MetadataService()
        rows = metadata_service.search(
            satellite_id=None,
            start_date=d_start,
            end_date=d_end,
            bounding_box=None,
            sql_filters=sql_filters)
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
        sql_filters = ['wrs_row=79']
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        metadata = Metadata(rows[0])
        self.assertEqual(len(metadata.get_file_list()), 0)

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
    def test_geometry(self):
        wrs_geometries = WRSGeometries()

        test_cases = [[15.74326, 26.98611, 1, 1, 1, 0, 13001, 13001, 13, 1, 'D', 1, 2233],
                      [2.74362, 6.65058, 942, 942, 1, 0, 61198, 61198, 61, 198, 'A', 1, 3174],
                      [13.37321, 24.20422, 2225, 2225, 1, 0, 125241, 125241, 125, 241, 'A', 2, 4209],
                      [3.58953, 7.7865, 1021, 1021, 1, 0, 75029, 75029, 75, 29, 'D', 3, 10445],
                      [4.2424, 8.69453, 891, 891, 1, 0, 64147, 64147, 64, 147, 'A', 6, 21227],
                      [16.81754, 27.20801, 3720, 3720, 1, 0, 223248, 223248, 223, 248, 'D', 16, 56296]]

        for test_case in test_cases:
            geom_obj = wrs_geometries.get_wrs_geometry(test_case[8], test_case[9], timeout=60)
            geom_expected_area = test_case[0]

            self.assertIsNotNone(geom_obj)
            s = shape(geom_obj)
            self.assertAlmostEqual(geom_expected_area, s.area, 5)
