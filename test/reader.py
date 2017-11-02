import unittest

from shapely.geometry import shape

from epl.imagery.reader import MetadataService, Landsat,\
    Storage, SpacecraftID, Metadata, BandMap, Band, \
    WRSGeometries, RasterBandMetadata, RasterMetadata, DataType, FunctionDetails


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
