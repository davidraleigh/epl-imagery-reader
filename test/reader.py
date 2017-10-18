import unittest
import datetime
import py_compile
import os

import numpy as np

import shapely.geometry
import requests

from math import isclose
from lxml import etree
from osgeo import gdal
from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata, BandMap, Band, WRSGeometries

from shapely.wkt import loads
from shapely.geometry import shape
from shapely.geometry import box


def text_compare(t1, t2, tolerance=None):
    if not t1 and not t2:
        return True
    if t1 == '*' or t2 == '*':
        return True
    if tolerance:
        try:
            t1_float = list(map(float, t1.split(",")))
            t2_float = list(map(float, t2.split(",")))
            if len(t1_float) != len(t2_float):
                return False

            for idx, val_1 in enumerate(t1_float):
                if not isclose(val_1, t2_float[idx], rel_tol=tolerance):
                    return False

            return True

        except:
            return False
    return (t1 or '').strip() == (t2 or '').strip()


# https://bitbucket.org/ianb/formencode/src/tip/formencode/doctest_xml_compare.py?fileviewer=file-view-default#cl-70
def xml_compare(x1, x2, tag_tolerances={}):
    tolerance = tag_tolerances[x1.tag] if x1.tag in tag_tolerances else None
    if x1.tag != x2.tag:
        return False, '\nTags do not match: %s and %s' % (x1.tag, x2.tag)
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            return False, '\nAttributes do not match: %s=%r, %s=%r' % (name, value, name, x2.attrib.get(name))
    for name in x2.attrib.keys():
        if name not in x1.attrib:
            return False, '\nx2 has an attribute x1 is missing: %s' % name
    if not text_compare(x1.text, x2.text, tolerance):
        return False, '\ntext: %r != %r, for tag %s' % (x1.text, x2.text, x1.tag)
    if not text_compare(x1.tail, x2.tail):
        return False, '\ntail: %r != %r' % (x1.tail, x2.tail)
    cl1 = sorted(x1.getchildren(), key=lambda x: x.tag)
    cl2 = sorted(x2.getchildren(), key=lambda x: x.tag)
    if len(cl1) != len(cl2):
        expected_tags = "\n".join(map(lambda x: x.tag, cl1)) + '\n'
        actual_tags = "\n".join(map(lambda x: x.tag, cl2)) + '\n'
        return False, '\nchildren length differs, %{0} != {1}\nexpected tags:\n{2}\nactual tags:\n{3}'.format(len(cl1), len(cl2), expected_tags, actual_tags)
    i = 0
    for c1, c2 in zip(cl1, cl2):
        i += 1
        result, message = xml_compare(c1, c2, tag_tolerances)
        # if not xml_compare(c1, c2):
        if not result:
            return False, '\nthe children %i do not match: %s\n%s' % (i, c1.tag, message)
    return True, "no errors"


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


class TestStorage(unittest.TestCase):
    base_mount_path = '/imagery'

    def test_storage_create(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])

        metadata = Metadata(rows[0], self.base_mount_path)
        self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))

    def test_singleton(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage_1 = Storage(gsurl[1])
        storage_2 = Storage(gsurl[1])
        self.assertTrue(storage_1 is storage_2)

    def test_delete_storage(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])

        metadata = Metadata(rows[0], self.base_mount_path)
        self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        files = [f for f in os.listdir(metadata.full_mount_path) if
                 os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        self.assertTrue(len(files) > 0)
        self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))
        files = [f for f in os.listdir(metadata.full_mount_path) if
                 os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        self.assertEqual(len(files), 0)
        self.assertTrue(storage.mount_sub_folder(metadata, "generic"))
        files = [f for f in os.listdir(metadata.full_mount_path) if
                 os.path.isfile(os.path.join(metadata.full_mount_path, f))]
        self.assertTrue(len(files) > 0)
        self.assertTrue(storage.unmount_sub_folder(metadata, "generic"))


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


class TestLandsat(unittest.TestCase):
    base_mount_path = '/imagery'
    metadata_service = None

    def setUp(self):
        self.metadata_service = MetadataService()

    def test_get_file(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        metadata = Metadata(rows[0], self.base_mount_path)
        landsat = Landsat(metadata)
        self.assertIsNotNone(landsat)
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'

    def test_gdal_info(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])

        metadata = Metadata(rows[0], self.base_mount_path)
        b_mounted = storage.mount_sub_folder(metadata, "generic")
        self.assertTrue(b_mounted)
        b_deleted = storage.unmount_sub_folder(metadata, "generic")
        self.assertTrue(b_deleted)

    def test_vrt(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['scene_id="LC80400312016103LGN00"']
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1, sql_filters=sql_filters)

        metadata = Metadata(rows[0], self.base_mount_path)

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
        sql_filters = ['wrs_row=79']
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        self.assertEqual(len(rows), 1)

        metadata = Metadata(rows[0])
        landsat = Landsat(metadata)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [3, 2, 1]
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        # nda = landsat.__get_ndarray(band_numbers, metadata, scaleParams)
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams)
        self.assertEqual((3581, 4046, 3), nda.shape)
        # print(nda.shape)

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
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        self.assertEqual(len(rows), 1)

        # data structure that contains all fields from Google's Landsat BigQuery Database
        metadata = Metadata(rows[0], self.base_mount_path)

        # GDAL helper functions for generating VRT
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([3, 2, 1])

        with open('testlandsat5.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10})
            self.assertTrue(result, message)

    def test_pixel_function_vrt_1(self):
        utah_box = (-112.66342163085938, 37.738141282210385, -111.79824829101562, 38.44821130413263)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)

        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=utah_box,
                                      limit=10, sql_filters=['collection_number=="PRE"', "cloud_cover<=5"])
        self.assertEqual(len(rows), 1)

        #     metadata_row = ['LC80390332016208LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2016-07-26',
        # '2016-07-26T18:14:46.9465460Z', 'PRE', 'N/A', 'L1T', 39, 33, 1.69,
        # 39.96962, 37.81744, -115.27267, -112.56732, 1070517542,
        # 'gs://gcp-public-data-landsat/LC08/PRE/039/033/LC80390332016208LGN00']
        metadata = Metadata(rows[0], self.base_mount_path)

        # GDAL helper functions for generating VRT
        landsat = Landsat(metadata)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [4, 3, 2]
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams)

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

    def test_unmount_destructor(self):
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        sql_filters = ['wrs_row=79']
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        metadata = Metadata(rows[0], self.base_mount_path)
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([4])
        storage = Storage("gcp-public-data-landsat")
        del landsat
        self.assertFalse(storage.is_mounted(metadata))

    def test_unmount_destructor_conflict(self):
        wkt = "POLYGON((136.2469482421875 -27.57843813308233,138.6639404296875 -27.57843813308233," \
              "138.6639404296875 -29.82351878748485,136.2469482421875 -29.82351878748485,136." \
              "2469482421875 -27.57843813308233))"

        polygon = loads(wkt)

        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        sql_filters = ['wrs_row=79']
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        metadata = Metadata(rows[0], self.base_mount_path)
        landsat = Landsat(metadata)
        vrt = landsat.get_vrt([4])
        storage = Storage("gcp-public-data-landsat")
        landsat_2 = Landsat(metadata)
        vrt = landsat_2.get_vrt([4])
        del landsat
        self.assertTrue(storage.is_mounted(metadata))

    def test_band_enum(self):
        self.assertTrue(True)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, limit=1, sql_filters=['scene_id="LC80390332016208LGN00"'])
        metadata = Metadata(rows[0])
        landsat = Landsat(metadata)
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        # nda = landsat.__get_ndarray(band_numbers, metadata, scaleParams)
        nda = landsat.fetch_imagery_array([Band.RED, Band.GREEN, Band.BLUE], scaleParams)
        self.assertIsNotNone(nda)
        nda2 = landsat.fetch_imagery_array([4, 3, 2], scaleParams)
        np.testing.assert_almost_equal(nda, nda2)
        # 'scene_id': 'LC80390332016208LGN00'

    def test_vrt_extent(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
        taos_geom = r.json()
        print(taos_geom)

        taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])

        metadata_service = MetadataService()

        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        sql_filters = ['collection_number="PRE"']
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=taos_shape.bounds,
            limit=10,
            sql_filters=sql_filters)
        print(len(rows))
        # mounted directory in docker container
        base_mount_path = '/imagery'

        metadataset = []
        for row in rows:
            metadataset.append(Metadata(row, base_mount_path))

        # GDAL helper functions for generating VRT
        landsat = Landsat(metadataset[0])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        vrt = landsat.get_vrt(band_numbers, extent=taos_shape.bounds)

        self.assertIsNotNone(vrt)


class TestPixelFunctions(unittest.TestCase):
    m_row_data = None
    base_mount_path = '/imagery'
    metadata_service = MetadataService()
    iowa_polygon = None

    def setUp(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['scene_id="LC80400312016103LGN00"']
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box,
                                       limit=1, sql_filters=sql_filters)
        self.m_row_data = rows[0]
        wkt_iowa = "POLYGON((-93.76075744628906 42.32707774458643,-93.47854614257812 42.32707774458643," \
                   "-93.47854614257812 42.12674735753131,-93.76075744628906 42.12674735753131," \
                   "-93.76075744628906 42.32707774458643))"
        self.iowa_polygon = loads(wkt_iowa)
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")

    def test_pixel_1(self):
        metadata = Metadata(self.m_row_data, self.base_mount_path)
        landsat = Landsat(metadata)  # , gsurl[2])

        code = """import numpy as np
def multiply_rounded(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,
                   raster_ysize, buf_radius, gt, **kwargs):
    factor = float(kwargs['factor'])
    out_ar[:] = np.round_(np.clip(in_ar[0] * factor,0,255))"""

        function_arguments = {"factor": "1.5"}
        pixel_function_details = {
            "band_numbers": [2],
            "function_code": code,
            "function_type": "multiply_rounded",
            "data_type": "Float32",
            "function_arguments": function_arguments
        }
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
        metadata = Metadata(self.m_row_data, self.base_mount_path)
        landsat = Landsat(metadata)  # , gsurl[2])

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

        pixel_function_details = {
            "band_numbers": [4, 5],
            "function_code": code,
            "function_type": "ndvi_numpy",
            "data_type": "UInt16",
        }
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
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        band_definitions = [pixel_function_details, 3, 2]
        nda = landsat.fetch_imagery_array(band_definitions, scaleParams)
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
        sql_filters = ["cloud_cover<=15"]
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)
        metadata = Metadata(rows[0], self.base_mount_path)
        landsat = Landsat(metadata)

        code = """import numpy as np
def ndvi_numpy(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    with np.errstate(divide = 'ignore', invalid = 'ignore'):
        out_ar[:] = np.divide((in_ar[1] - in_ar[0]), (in_ar[1] + in_ar[0]))
        out_ar[np.isnan(out_ar)] = 0.0
        out_ar """

        pixel_function_details = {
            "band_numbers": [4, 5],
            "function_code": code,
            "function_type": "ndvi_numpy",
            "data_type": "Float32",
        }

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
        sql_filters = ["cloud_cover<=15"]
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)
        metadata = Metadata(rows[0], self.base_mount_path)
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

        pixel_function_details = {
            "function_arguments": {"factor": 255},
            "band_numbers": [4, Band.NIR],
            "function_code": code,
            "function_type": "ndvi_numpy",
            "data_type": "Float32",
        }

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
        local_ndvi *= pixel_function_details['function_arguments']['factor'] / 2.0
        self.assertFalse(np.any(np.isinf(local_ndvi)))

        np.floor(arr_ndvi, out=arr_ndvi)
        np.floor(local_ndvi, out=local_ndvi)
        np.testing.assert_almost_equal(arr_ndvi, local_ndvi, decimal=0)

    def test_malformed_funciton(self):
        d_start = date(2016, 4, 4)
        d_end = date(2016, 8, 7)
        bounding_box = self.iowa_polygon.bounds
        sql_filters = ["cloud_cover<=15"]
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)
        metadata = Metadata(rows[0], self.base_mount_path)
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
            out_ar *= factor/2.0 """

        pixel_function_details = {
            "function_arguments": {"factor": 255},
            "band_numbers": [4, 5],
            "function_code": code,
            "function_type": "ndvi_numpy",
            "data_type": "Float32",
        }

        band_definitions = [pixel_function_details, 4, 5]
        self.assertRaises(py_compile.PyCompileError, lambda: landsat.get_vrt(band_definitions))


    # def test_translate_vrt(self):
    #     #                                                          LC80390332016208LGN00
    """
    gdalbuildvrt -separate rgb.vrt /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B4.TIF \
    /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B3.TIF \
    /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B2.TIF
    """
    #     # gdal_translate -of VRT -ot Byte -scale -tr 60 60 rgb.vrt rgb_byte_scaled.vrt
    #
    #     self.assertTrue(True)
    #     sql_filters = ['scene_id="LC80390332016208LGN00"']
    #     metadata_service = MetadataService()
    #     rows = metadata_service.search(SpacecraftID.LANDSAT_8, sql_filters=sql_filters)
    #
    #
    #     metadata = Metadata(rows[0], self.base_mount_path)
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
