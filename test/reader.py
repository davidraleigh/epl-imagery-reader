import unittest
import datetime

from lxml import etree
from osgeo import gdal
from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata


def text_compare(t1, t2):
    if not t1 and not t2:
        return True
    if t1 == '*' or t2 == '*':
        return True
    return (t1 or '').strip() == (t2 or '').strip()


# https://bitbucket.org/ianb/formencode/src/tip/formencode/doctest_xml_compare.py?fileviewer=file-view-default#cl-70
def xml_compare(x1, x2):
    if x1.tag != x2.tag:
        return False, 'Tags do not match: %s and %s' % (x1.tag, x2.tag)
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            return False, 'Attributes do not match: %s=%r, %s=%r' % (name, value, name, x2.attrib.get(name))
    for name in x2.attrib.keys():
        if name not in x1.attrib:
            return False, 'x2 has an attribute x1 is missing: %s' % name
    if not text_compare(x1.text, x2.text):
        return False, 'text: %r != %r' % (x1.text, x2.text)
    if not text_compare(x1.tail, x2.tail):
        return False, 'tail: %r != %r' % (x1.tail, x2.tail)
    cl1 = x1.getchildren()
    cl2 = x2.getchildren()
    if len(cl1) != len(cl2):
        return False, 'children length differs, %i != %i' % (len(cl1), len(cl2))
    i = 0
    for c1, c2 in zip(cl1, cl2):
        i += 1
        if not xml_compare(c1, c2):
            return False, 'children %i do not match: %s' % (i, c1.tag)
    return True

class TestMetaDataSQL(unittest.TestCase):
    def test_start_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadataService = MetadataService()
        d = date(2016, 6, 24)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertGreaterEqual(d_actual, d)

    def test_end_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadataService = MetadataService()
        d = date(2016, 6, 24)
        rows = metadataService.search(SpacecraftID.LANDSAT_7, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_7.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d)

    def test_one_day(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadataService = MetadataService()
        d = date(2016, 6, 24)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertEqual(d_actual, d)

    def test_1_year(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            self.assertTrue((bounding_box[0] < row[14] < bounding_box[2]) or (bounding_box[0] < row[15] < bounding_box[2]))
            self.assertTrue((bounding_box[1] < row[12] < bounding_box[3]) or (bounding_box[1] < row[13] < bounding_box[3]))


class TestStorage(unittest.TestCase):
    def test_storage_create(self):
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'
        self.assertTrue(storage.mount_sub_folder(gsurl[2], base_mount_path))


class TestLandsat(unittest.TestCase):
    def test_get_file(self):
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        landsat = Landsat('/data/imagery')
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'

    def test_gdal_info(self):
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_mount_path = '/imagery'
        b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
        self.assertTrue(b_mounted)

    def test_vrt(self):
        metadataService = MetadataService()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box,
                               limit=1)
        base_mount_path = '/imagery'
        metadata = Metadata(rows[0], base_mount_path)
        gsurl = urlparse(metadata.base_url)
        storage = Storage(gsurl[1])

        b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
        landsat = Landsat(base_mount_path, gsurl[2])
        vrt = landsat.get_vrt(metadata, [5,4,3])
        with open('test_1.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML('<xml>%s</xml>' % data)
            actual = etree.XML('<xml>%s</xml>' % vrt)
            result, message = xml_compare(expected, actual)
            self.assertTrue(result, message)



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



