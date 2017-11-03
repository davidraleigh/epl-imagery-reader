import unittest
from epl.imagery import PLATFORM_PROVIDER


class TestAWSStorage(unittest.TestCase):
    def test_mount(self):
        self.assertEqual("AWS", PLATFORM_PROVIDER)
        self.assertTrue(True)
