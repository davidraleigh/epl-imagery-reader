import unittest

import datetime

from datetime import date

from epl.imagery import PLATFORM_PROVIDER
from epl.imagery.reader import MetadataService, SpacecraftID, Metadata


class TestAWSStorage(unittest.TestCase):
    def test_mount(self):
        self.assertEqual("AWS", PLATFORM_PROVIDER)
        self.assertTrue(True)


class TestAWSMetadata(unittest.TestCase):
    def test_aws_file_path(self):
        # PRE        s3://landsat-pds/L8/139/045/LC81390452014295LGN00/
        # non-PRE s3://landsat-pds/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1/
        metadataservice = MetadataService()
        start_date = datetime.datetime.strptime('14295', '%y%j').date()
        rows = metadataservice.search(SpacecraftID.LANDSAT_8,
                                      start_date=start_date,
                                      end_date=start_date,
                                      sql_filters=["wrs_path=139", "wrs_row=45", "collection_number='PRE'"])
        self.assertEqual(len(rows), 1)
        metadata = Metadata(rows[0])
        self.assertEqual(metadata.get_aws_file_path(), "/imagery/L8/139/045/LC81390452014295LGN00")

        rows = metadataservice.search(SpacecraftID.LANDSAT_8,
                                      start_date=date(2017, 3, 4),
                                      end_date=date(2017, 3, 4),
                                      sql_filters=["wrs_path=139", "wrs_row=45", "collection_number!='PRE'"])
        self.assertEqual(len(rows), 1)
        metadata = Metadata(rows[0])
        self.assertEqual(metadata.get_aws_file_path(),
                         "/imagery/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1")

    def test_google_aws_mismatch(self):
        metadata_service = MetadataService()
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        sql_filters = ['collection_number!="PRE"']
        rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                       start_date=d_start,
                                       end_date=d_end,
                                       bounding_box=bounding_box,
                                       limit=1,
                                       sql_filters=sql_filters)

        metadata = Metadata(rows[0], '/imagery')
        self.assertNotEqual(rows[0][1], metadata.product_id)
        self.assertNotEqual(rows[0][7], metadata.collection_category)