import unittest
import datetime

from lxml import etree
from osgeo import gdal
from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata


def text_compare(t1, t2, compare_as_float=False):
    if not t1 and not t2:
        return True
    if t1 == '*' or t2 == '*':
        return True
    if compare_as_float:
        try:
            t1_float = map(lambda x: float(x), t1.split(","))
            t2_float = map(lambda x: float(x), t2.split(","))
            if len(t1_float) != len(t2_float):
                return False

        except:
            return False
    return (t1 or '').strip() == (t2 or '').strip()


# https://bitbucket.org/ianb/formencode/src/tip/formencode/doctest_xml_compare.py?fileviewer=file-view-default#cl-70
def xml_compare(x1, x2, float_text_tags={}):
    if x1.tag != x2.tag:
        return False, 'Tags do not match: %s and %s' % (x1.tag, x2.tag)
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            return False, 'Attributes do not match: %s=%r, %s=%r' % (name, value, name, x2.attrib.get(name))
    for name in x2.attrib.keys():
        if name not in x1.attrib:
            return False, 'x2 has an attribute x1 is missing: %s' % name
    if not text_compare(x1.text, x2.text, x1.tag in float_text_tags):
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
        result, message = xml_compare(c1, c2)
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
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadata_service = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
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
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
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
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'
        self.assertTrue(storage.mount_sub_folder(gsurl[2], base_mount_path))


class TestLandsat(unittest.TestCase):
    def test_get_file(self):
        metadata_service = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        landsat = Landsat('/data/imagery')
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'

    def test_gdal_info(self):
        metadata_service = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'
        b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
        self.assertTrue(b_mounted)

    def test_vrt(self):
        metadata_service = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['scene_id="LC80400312016103LGN00"']
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1, sql_filters=sql_filters)


        base_mount_path = '/imagery'
        metadata = Metadata(rows[0], base_mount_path)
        gsurl = urlparse(metadata.base_url)
        storage = Storage(gsurl[1])

        b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
        landsat = Landsat(base_mount_path)#, gsurl[2])
        vrt = landsat.get_vrt(metadata, [4, 3, 2])
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
    #     # gdalbuildvrt -separate rgb.vrt /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B4.TIF /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B3.TIF /imagery/LC08/PRE/039/033/LC80390332016208LGN00/LC80390332016208LGN00_B2.TIF
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


