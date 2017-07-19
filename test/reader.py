import unittest
import datetime
import os

from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import Metadata, Landsat, Storage, SpacecraftID


class TestMetaDataSQL(unittest.TestCase):
    def test_start_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata = Metadata()
        d = date(2016, 6, 24)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertGreaterEqual(d_actual, d)

    def test_end_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata = Metadata()
        d = date(2016, 6, 24)
        rows = metadata.search(SpacecraftID.LANDSAT_7, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_7.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d)

    def test_one_day(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata = Metadata()
        d = date(2016, 6, 24)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d, end_date=d)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertEqual(d_actual, d)

    def test_1_year(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata = Metadata()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadata = Metadata()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box)
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
        metadata = Metadata()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        path = rows[0][17]
        gsurl = urlparse(path)
        storage = Storage(gsurl[1])
        base_path = '/imagery'
        # full_path = os.path.join('/imagery', gsurl[2])
        self.assertTrue(storage.mount_sub_folder(gsurl[2], base_path))


class TestLandsat(unittest.TestCase):
    def test_get_file(self):
        metadata = Metadata()
        d_end = date(2016, 6, 24)
        d_start = date(2015, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = metadata.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        landsat = Landsat('/data/imagery')
        #    'gs://gcp-public-data-landsat/LC08/PRE/037/036/LC80370362016082LGN00'



