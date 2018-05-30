import unittest
from datetime import date

from epl.imagery.native.metadata_helpers import _RangeQueryParam, _DateQueryParam, _QueryParam, LandsatQueryFilters, MetadataFilters, LandsatModel


expected_prefix = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 WHERE '


class TestMetadata(unittest.TestCase):
    def test_peewee_1(self):
        a = LandsatQueryFilters()
        a.scene_id.set_value("LC80270312016188LGN00")
        # sql_message = a.scene_id.get()
        sql_stuff = a.get_sql()
        self.assertIsNotNone(sql_stuff)
        self.maxDiff = None
        expected = "{}{}".format(expected_prefix,'(t1.scene_id IN ("LC80270312016188LGN00")) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.assertEqual(expected, sql_stuff)

    def test_not_in_in(self):
        lqf = LandsatQueryFilters()
        lqf.cloud_cover.set_not_value(44)
        lqf.cloud_cover.set_not_value(48)
        lqf.cloud_cover.set_not_value(50)
        lqf.cloud_cover.set_value(64)
        lqf.cloud_cover.set_value(68)
        lqf.cloud_cover.set_value(70)

        lqf.cloud_cover.set_range(start=99, end_inclusive=False, end=42)
        sql = lqf.get_sql()
        expected = "{}{}".format(expected_prefix, "((((t1.cloud_cover IN (64.0, 68.0, 70.0)) AND NOT (t1.cloud_cover IN (44.0, 48.0, 50.0))) AND (t1.cloud_cover >= 99.0)) AND (t1.cloud_cover < 42.0)) ORDER BY t1.sensing_time DESC LIMIT 10")
        self.maxDiff = None
        self.assertMultiLineEqual(expected, sql)

    def test_or_and(self):
        lqf = LandsatQueryFilters()
        lqf.cloud_cover.set_not_value(44)
        lqf.cloud_cover.set_range(start=99, end_inclusive=False, end=42)
        sql = lqf.get_sql()
        expected = "{}{}".format(expected_prefix, "((NOT (t1.cloud_cover IN (44.0)) AND (t1.cloud_cover >= 99.0)) AND (t1.cloud_cover < 42.0)) ORDER BY t1.sensing_time DESC LIMIT 10")
        self.maxDiff = None
        self.assertMultiLineEqual(expected, sql)

    def test_query_param_landsat(self):
        a = LandsatQueryFilters()
        a.scene_id.set_value("LC80330352017072LGN00")
        a.cloud_cover.set_range(None, None, 2, False)
        a.collection_number.set_value("PRE")
        a.wrs_path.set_value(33)

        result = a.get_sql(sort_by_field=a.acquired.field)
        self.assertEqual('SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 WHERE ((((t1.cloud_cover < 2.0) AND (t1.collection_number IN ("PRE"))) AND (t1.scene_id IN ("LC80330352017072LGN00"))) AND (t1.wrs_path IN (33))) ORDER BY t1.sensing_time LIMIT 10', result)

        result = a.get_sql()
        self.assertEqual('SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 WHERE ((((t1.cloud_cover < 2.0) AND (t1.collection_number IN ("PRE"))) AND (t1.scene_id IN ("LC80330352017072LGN00"))) AND (t1.wrs_path IN (33))) ORDER BY t1.sensing_time DESC LIMIT 10',result)

    def test_dates(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(d_start, True, d_end, True)

        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, '(((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-19T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

        landsat_filters.acquired.set_range(start=d_start, start_inclusive=True)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, '((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

        landsat_filters.acquired.set_range(end=d_end, end_inclusive=True)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, '((t1.sensing_time <= "2017-03-19T23:59:59.999999") AND (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_dates_2(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_value(d_start)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, '(((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-12T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.assertEqual(expected, result)

    def test_dates_3(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_not_value(d_start)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, '(((t1.sensing_time > "2017-03-12T23:59:59.999999") AND (t1.sensing_time < "2017-03-12T00:00:00")) AND (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_bounding_box(self):
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['collection_number!="PRE"']
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_not_value("PRE")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.bounds.set_bounds(*bounding_box)
        result = landsat_filter.get_sql()
        expected = "{}{}".format(expected_prefix, '((((t1.sensing_time >= "2017-06-24T00:00:00") AND (t1.sensing_time <= "2017-09-24T23:59:59.999999")) AND ((((t1.west_lon >= -115.927734375) & (t1.west_lon <= -78.31054687499999)) | ((t1.west_lon <= -115.927734375) & (t1.east_lon >= -115.927734375))) & (((t1.south_lat <= 34.52466147177172) & (t1.north_lat >= 34.52466147177172)) | ((t1.south_lat > 34.52466147177172) & (t1.south_lat <= 44.84029065139799))))) AND NOT (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)
