import unittest
import requests
import shapely.geometry

from datetime import date

from epl.imagery import PLATFORM_PROVIDER
from epl.imagery.reader import Metadata, MetadataService, SpacecraftID, Landsat


class TestAWSStorage(unittest.TestCase):
    def test_mount(self):
        self.assertEqual("AWS", PLATFORM_PROVIDER)
        self.assertTrue(True)


class TestAWSLandsat(unittest.TestCase):
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
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=self.taos_shape.bounds,
            limit=10,
            sql_filters=sql_filters)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        for row in rows:
            self.metadata_set.append(Metadata(row, base_mount_path))

    def test_get_file(self):
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=bounding_box, limit=1)
        metadata = Metadata(rows[0], self.base_mount_path)
        landsat = Landsat(metadata)
        self.assertIsNotNone(landsat)