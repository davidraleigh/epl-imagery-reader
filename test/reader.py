import unittest
from epl.imagery.reader import Metadata, Landsat, Storage, SatelliteID


class TestLandsat(unittest.TestCase):
    def test_url_landsat_8(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        landsat = Landsat("/landsat")
        metadata = Metadata()
        storage = Storage("gcp-public-data-landsat")
        self.assertEqual(len(metadata.search(SatelliteID.LANDSAT_8)), 10)
