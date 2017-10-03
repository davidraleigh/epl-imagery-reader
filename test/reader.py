import unittest
import datetime

from math import isclose
from lxml import etree
from osgeo import gdal
from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata, BandMap

from shapely.wkt import loads


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
        return False, 'Tags do not match: %s and %s' % (x1.tag, x2.tag)
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            return False, 'Attributes do not match: %s=%r, %s=%r' % (name, value, name, x2.attrib.get(name))
    for name in x2.attrib.keys():
        if name not in x1.attrib:
            return False, 'x2 has an attribute x1 is missing: %s' % name
    if not text_compare(x1.text, x2.text, tolerance):
        return False, 'text: %r != %r, for tag %s' % (x1.text, x2.text, x1.tag)
    if not text_compare(x1.tail, x2.tail):
        return False, 'tail: %r != %r' % (x1.tail, x2.tail)
    cl1 = sorted(x1.getchildren(), key=lambda x: x.tag)
    cl2 = sorted(x2.getchildren(), key=lambda x: x.tag)
    if len(cl1) != len(cl2):
        return False, 'children length differs, %i != %i' % (len(cl1), len(cl2))
    i = 0
    for c1, c2 in zip(cl1, cl2):
        i += 1
        result, message = xml_compare(c1, c2, tag_tolerances)
        # if not xml_compare(c1, c2):
        if not result:
            return False, 'children %i do not match: %s\n%s' % (i, c1.tag, message)
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


class TestStorage(unittest.TestCase):
    def test_storage_create(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'

        metadata = Metadata(rows[0], base_mount_path)
        self.assertTrue(storage.mount_sub_folder(metadata))


class TestBandMap(unittest.TestCase):
    def test_landsat_5(self):
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertEqual(band_map.get_band_number("Blue"), 1)
        self.assertEqual(band_map.get_band_number("SWIR2"), 7)
        self.assertEqual(band_map.get_band_number("Thermal"), 6)

        self.assertEqual(band_map.get_band_name(1), "Blue")
        self.assertEqual(band_map.get_band_name(7), "SWIR2")
        self.assertEqual(band_map.get_band_name(6), "Thermal")

        for idx, val in enumerate(["Blue", "Green", "Red", "NIR", "SWIR1"]):
            self.assertEqual(band_map.get_band_name(idx + 1), val)
            self.assertEqual(band_map.get_band_number(val), idx + 1)

    def test_landsat_5_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertRaises(KeyError, lambda: band_map.get_band_number("Cirrus"))
        self.assertRaises(KeyError, lambda: band_map.get_band_number("Panchromatic"))
        self.assertRaises(KeyError, lambda: band_map.get_band_name(8))

    def test_landsat_7(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertEqual(band_map.get_band_number("Blue"), 1)
        self.assertEqual(band_map.get_band_number("SWIR1"), 5)
        self.assertEqual(band_map.get_band_number("Thermal"), 6)

        self.assertEqual(band_map.get_band_name(1), "Blue")
        self.assertEqual(band_map.get_band_name(5), "SWIR1")
        self.assertEqual(band_map.get_band_name(6), "Thermal")
        self.assertEqual(band_map.get_band_number("SWIR2"), 7)
        self.assertEqual(band_map.get_band_name(7), "SWIR2")

        for idx, val in enumerate(["Blue", "Green", "Red", "NIR", "SWIR1"]):
            self.assertEqual(band_map.get_band_name(idx + 1), val)
            self.assertEqual(band_map.get_band_number(val), idx + 1)

    def test_landsat_7_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertRaises(KeyError, lambda: band_map.get_band_number("Cirrus"))
        self.assertRaises(KeyError, lambda: band_map.get_band_number("TIRS1"))
        self.assertRaises(KeyError, lambda: band_map.get_band_name(9))

    def test_landsat_8(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertEqual(band_map.get_band_number("Blue"), 2)
        self.assertEqual(band_map.get_band_number("SWIR1"), 6)

        self.assertEqual(band_map.get_band_name(2), "Blue")
        self.assertEqual(band_map.get_band_name(6), "SWIR1")

        self.assertEqual(band_map.get_band_number("SWIR2"), 7)
        self.assertEqual(band_map.get_band_name(7), "SWIR2")

        for idx, val in enumerate(["Blue", "Green", "Red", "NIR", "SWIR1"]):
            self.assertEqual(band_map.get_band_name(idx + 2), val)
            self.assertEqual(band_map.get_band_number(val), idx + 2)

        self.assertEqual(band_map.get_band_number("Cirrus"), 9)
        self.assertEqual(band_map.get_band_number("TIRS1"), 10)
        self.assertEqual(band_map.get_band_number("TIRS2"), 11)

        self.assertEqual(band_map.get_band_name(9), "Cirrus")
        self.assertEqual(band_map.get_band_name(10), "TIRS1")
        self.assertEqual(band_map.get_band_name(11), "TIRS2")

    def test_landsat_8_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertRaises(KeyError, lambda: band_map.get_band_number("Thermal"))
        self.assertRaises(KeyError, lambda: band_map.get_band_name(12))


class TestLandsat(unittest.TestCase):
    def test_get_file(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        metadata = Metadata(rows[0], '/data/imagery')
        landsat = Landsat(metadata)
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'

    def test_gdal_info(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'
        metadata = Metadata(rows[0], base_mount_path)
        b_mounted = storage.mount_sub_folder(metadata)
        self.assertTrue(b_mounted)

    def test_vrt(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['scene_id="LC80400312016103LGN00"']
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1, sql_filters=sql_filters)


        base_mount_path = '/imagery'
        metadata = Metadata(rows[0], base_mount_path)
        gsurl = urlparse(metadata.base_url)
        storage = Storage(gsurl[1])

        b_mounted = storage.mount_sub_folder(metadata)
        self.assertTrue(b_mounted)
        landsat = Landsat(metadata)#, gsurl[2])
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

        metadata_service = MetadataService()
        # sql_filters = ['cloud_cover=0']
        d_start = date(2006, 8, 4)
        d_end = date(2006, 8, 7)
        bounding_box = polygon.bounds
        sql_filters = ['wrs_row=79']
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_5,
            start_date=d_start,
            end_date=d_end,
            bounding_box=bounding_box,
            sql_filters=sql_filters)

        self.assertEqual(len(rows), 1)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        # data structure that contains all fields from Google's Landsat BigQuery Database
        metadata = Metadata(rows[0], base_mount_path)
        # print(metadata.__dict__)

        # break down gs url into pieces required for gcs-fuse
        # gsurl = urlparse(metadata.base_url)

        # mounting Google Storage bucket with gcs-fuse
        # storage = Storage(gsurl[1])
        # b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)

        # print(gsurl[1])
        # print(gsurl[2])
        # GDAL helper functions for generating VRT
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

        self.assertEqual(len(rows), 1)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        # data structure that contains all fields from Google's Landsat BigQuery Database
        metadata = Metadata(rows[0], base_mount_path)
        # print(metadata.__dict__)

        # break down gs url into pieces required for gcs-fuse
        gsurl = urlparse(metadata.base_url)

        # mounting Google Storage bucket with gcs-fuse
        storage = Storage(gsurl[1])
        b_mounted = storage.mount_sub_folder(metadata)
        self.assertTrue(b_mounted)

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

        metadata_service = MetadataService()
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=utah_box,
                                      limit=10, sql_filters=['collection_number=="PRE"', "cloud_cover<=5"])
        self.assertEqual(len(rows), 1)
        base_mount_path = '/imagery'
        #     metadata_row = ['LC80390332016208LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2016-07-26',
        # '2016-07-26T18:14:46.9465460Z', 'PRE', 'N/A', 'L1T', 39, 33, 1.69,
        # 39.96962, 37.81744, -115.27267, -112.56732, 1070517542,
        # 'gs://gcp-public-data-landsat/LC08/PRE/039/033/LC80390332016208LGN00']
        metadata = Metadata(rows[0], base_mount_path)
        # break down gs url into pieces required for gcs-fuse
        # gsurl = urlparse(metadata.base_url)
        #
        # # mounting Google Storage bucket with gcs-fuse
        # storage = Storage(gsurl[1])
        # b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
        # self.assertTrue(b_mounted)
        #
        # storage = Storage(gsurl[1])
        # GDAL helper functions for generating VRT
        landsat = Landsat(metadata)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [4, 3, 2]
        scaleParams = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams)
        # nda = landsat.__get_ndarray(band_numbers, metadata, scaleParams)

        # landsat = Landsat(base_mount_path)  # , gsurl[2])
        vrt = landsat.get_vrt([4, 3, 2])
    #     self.assertTrue(True)
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
    #     base_mount_path = '/imagery'
    #     metadata = Metadata(rows[0], base_mount_path)
    #     gsurl = urlparse(metadata.base_url)
    #     storage = Storage(gsurl[1])
    #
    #     b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
    #     landsat = Landsat(base_mount_path, gsurl[2])
    #     vrt = landsat.get_vrt(metadata, [5, 4, 3])
    #
    #     with open('gdalbuildvrt_LC80390332016208LGN00.vrt', 'r') as myfile:
    #         data = myfile.read()
    #         expected = etree.XML(data)
    #         actual = etree.XML(vrt)
    #         result, message = xml_compare(expected, actual)
    #         self.assertTrue(result, message)


