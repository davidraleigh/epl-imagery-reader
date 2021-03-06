import requests
import unittest

from shapely.geometry import box
from shapely.wkt import loads as loads_wkt
from shapely.wkb import loads as loads_wkb
from shapely.geometry import shape
from shapely.geometry import box
from datetime import date, datetime

from epl.grpc.geometry.geometry_operators_pb2 import SpatialReferenceData
from epl.grpc.imagery import epl_imagery_pb2
from epl.native.imagery.metadata_helpers import LandsatQueryFilters, MetadataFilters, LandsatModel, SpacecraftID


expected_prefix = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1'


class TestMetadata(unittest.TestCase):
    def test_peewee_1(self):
        a = LandsatQueryFilters()
        a.scene_id.set_value("LC80270312016188LGN00")
        # sql_message = a.scene_id.get()
        sql_stuff = a.get_sql()
        self.assertIsNotNone(sql_stuff)
        self.maxDiff = None
        expected = "{}{}".format(expected_prefix, ' WHERE (t1.scene_id IN ("LC80270312016188LGN00")) LIMIT 10')
        self.assertMultiLineEqual(expected, sql_stuff)

    def test_not_in_in(self):
        lqf = LandsatQueryFilters()
        lqf.cloud_cover.set_exclude_value(44)
        lqf.cloud_cover.set_exclude_value(48)
        lqf.cloud_cover.set_exclude_value(50)
        lqf.cloud_cover.set_value(64)
        lqf.cloud_cover.set_value(68)
        lqf.cloud_cover.set_value(70)
        lqf.east_lon.sort_by(epl_imagery_pb2.DESCENDING)

        lqf.cloud_cover.set_range(end=99, end_inclusive=False, start=42)
        sql = lqf.get_sql()
        expected = "{}{}".format(expected_prefix, " WHERE (((t1.cloud_cover IN (64.0, 68.0, 70.0)) AND NOT (t1.cloud_cover IN (44.0, 48.0, 50.0))) AND ((t1.cloud_cover >= 42.0) AND (t1.cloud_cover < 99.0))) ORDER BY t1.east_lon DESC LIMIT 10")
        self.maxDiff = None
        self.assertMultiLineEqual(expected, sql)

    def test_or_and(self):
        lqf = LandsatQueryFilters()
        lqf.cloud_cover.set_exclude_value(44)
        lqf.cloud_cover.set_range(end=42, end_inclusive=True)
        lqf.cloud_cover.set_range(start=46, start_inclusive=True)

        sql = lqf.get_sql()
        expected = "{}{}".format(expected_prefix, " WHERE (NOT (t1.cloud_cover IN (44.0)) AND ((t1.cloud_cover <= 42.0) OR (t1.cloud_cover >= 46.0))) LIMIT 10")
        self.maxDiff = None
        self.assertMultiLineEqual(expected, sql)

    def test_query_param_landsat(self):
        a = LandsatQueryFilters()
        a.scene_id.set_value("LC80330352017072LGN00")
        a.cloud_cover.set_range(end=2, end_inclusive=False)
        a.collection_number.set_value("PRE")
        a.total_size.sort_by(epl_imagery_pb2.ASCENDING)
        a.wrs_path.set_value(33)

        result = a.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.cloud_cover < 2.0) AND (t1.collection_number IN ("PRE"))) AND (t1.scene_id IN ("LC80330352017072LGN00"))) AND (t1.wrs_path IN (33))) ORDER BY t1.total_size LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

        result = a.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.cloud_cover < 2.0) AND (t1.collection_number IN ("PRE"))) AND (t1.scene_id IN ("LC80330352017072LGN00"))) AND (t1.wrs_path IN (33))) ORDER BY t1.total_size LIMIT 10')
        self.assertMultiLineEqual(expected, result)

    def test_dates(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(d_start, True, d_end, True)

        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE (((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-19T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

        d_start = date(2017, 3, 24)  # 2017-03-12
        landsat_filters.acquired.set_range(start=d_start, start_inclusive=True)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-19T23:59:59.999999")) OR (t1.sensing_time >= "2017-03-24T00:00:00")) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

        d_end = date(2017, 3, 9)
        landsat_filters.acquired.set_range(end=d_end, end_inclusive=True)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE (((((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-19T23:59:59.999999")) OR (t1.sensing_time >= "2017-03-24T00:00:00")) OR (t1.sensing_time <= "2017-03-09T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_dates_2(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_value(d_start)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE (((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-12T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_dates_3(self):
        # TODO include in metadata tests and check dates (should only be the 10, 11 and 13th - 17th
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(start=date(2017, 3, 10), end=date(2017, 3, 17))
        landsat_filters.acquired.set_exclude_value(d_start)
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.sensing_time >= "2017-03-10T00:00:00") AND (t1.sensing_time <= "2017-03-17T23:59:59.999999")) AND NOT ((t1.sensing_time >= "2017-03-12T00:00:00") AND (t1.sensing_time <= "2017-03-12T23:59:59.999999"))) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_dates_4(self):
        # TODO include in metadata tests and check dates (should only be the 10th and the 17th
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(start=date(2017, 3, 10), end=date(2017, 3, 17))
        landsat_filters.acquired.set_exclude_range(start=date(2017, 3, 11), end=date(2017, 3, 16))
        result = landsat_filters.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.sensing_time >= "2017-03-10T00:00:00") AND (t1.sensing_time <= "2017-03-17T23:59:59.999999")) AND NOT ((t1.sensing_time >= "2017-03-11T00:00:00") AND (t1.sensing_time <= "2017-03-16T23:59:59.999999"))) AND (t1.collection_number IN ("PRE"))) LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_bounding_box(self):
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['collection_number!="PRE"']
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_exclude_value("PRE")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.aoi.set_bounds(*bounding_box)
        landsat_filter.acquired.sort_by(epl_imagery_pb2.DESCENDING)
        result = landsat_filter.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE ((((t1.sensing_time >= "2017-06-24T00:00:00") AND (t1.sensing_time <= "2017-09-24T23:59:59.999999")) AND ((((t1.west_lon >= -115.927734375) AND (t1.west_lon <= -78.31054687499999)) OR ((t1.west_lon <= -115.927734375) AND (t1.east_lon >= -115.927734375))) AND (((t1.south_lat <= 34.52466147177172) AND (t1.north_lat >= 34.52466147177172)) OR ((t1.south_lat > 34.52466147177172) AND (t1.south_lat <= 44.84029065139799))))) AND NOT (t1.collection_number IN ("PRE"))) ORDER BY t1.sensing_time DESC LIMIT 10')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, result)

    def test_query_filter_from_grpc(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_exclude_value(d_start)
        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.assertMultiLineEqual(expected, expected_2)

        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        landsat_filters.acquired.set_range(start=d_start, end=d_end)
        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.assertMultiLineEqual(expected, expected_2)

    def test_grpc_bounds(self):
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_exclude_value(d_start)
        landsat_filters.aoi.set_bounds(*bounding_box)
        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

    def test_wkb_added(self):
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        polygon = box(*bounding_box).envelope
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_exclude_value(d_start)
        landsat_filters.aoi.set_geometry(polygon.wkb)
        # landsat_filters.geometry_bag.geometry_binaries.append(polygon.wkb)

        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

        self.assertMultiLineEqual(polygon.wkt, loads_wkb(landsat_filters_2.aoi.query_params.geometry_bag.geometry_binaries[0]).wkt)

    def test_bounds_spatial_reference(self):
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        d_start = date(2017, 3, 12)  # 2017-03-12
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_exclude_value(d_start)
        landsat_filters.aoi.set_bounds(*bounding_box, spatial_reference=SpatialReferenceData(wkid=4326))
        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

        self.assertEqual(4326, landsat_filters_2.aoi.query_params.bounds[0].spatial_reference.wkid)

    def test_dateline(self):
        bounding_box = (-115, 34, -118, 44)
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.aoi.set_bounds(*bounding_box)
        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql()
        #
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

        expected = landsat_filters.get_sql()

        query_filter = landsat_filters.get_query_filter()

    def test_wrs_path_row_pairs(self):
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.wrs_path_row.set_pair(23, 45)
        landsat_filters.wrs_path_row.set_pair(46, 48)
        landsat_filters.wrs_path_row.set_pair(89, 90)
        landsat_filters.cloud_cover.sort_by(epl_imagery_pb2.ASCENDING)
        actual = landsat_filters.get_sql(limit=20)
        expected = "{}{}".format(expected_prefix, ' WHERE ((t1.collection_number IN ("PRE")) AND ((((t1.wrs_path = 23) & (t1.wrs_row = 45)) OR ((t1.wrs_path = 46) & (t1.wrs_row = 48))) OR ((t1.wrs_path = 89) & (t1.wrs_row = 90)))) ORDER BY t1.cloud_cover LIMIT 20')
        self.maxDiff = None
        self.assertMultiLineEqual(expected, actual)
        query_filter = landsat_filters.get_query_filter()

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql(limit=20)
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

        landsat_filters_2 = LandsatQueryFilters(query_filter=query_filter)
        expected_2 = landsat_filters_2.get_sql(limit=20)
        landsat_filters_2.wrs_path.sort_by(epl_imagery_pb2.ASCENDING)
        self.maxDiff = None
        self.assertMultiLineEqual(expected, expected_2)

    def test_select_all(self):
        expected_prefix = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 '
        a = LandsatQueryFilters()
        # sql_message = a.scene_id.get()
        sql_stuff = a.get_sql()
        self.assertIsNotNone(sql_stuff)
        self.maxDiff = None

        expected = "{}{}".format(expected_prefix, 'LIMIT 10')
        self.assertMultiLineEqual(expected, sql_stuff)

    def test_sort_by_one_field(self):
        expected_prefix = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 '
        a = LandsatQueryFilters()
        a.cloud_cover.sort_by(epl_imagery_pb2.ASCENDING)
        sql_stuff = a.get_sql()
        self.assertIsNotNone(sql_stuff)
        self.maxDiff = None
        expected = "{}{}".format(expected_prefix, 'ORDER BY t1.cloud_cover LIMIT 10')
        self.assertMultiLineEqual(expected, sql_stuff)

        a.east_lon.sort_by(epl_imagery_pb2.ASCENDING)
        sql_stuff = a.get_sql()
        self.assertIsNotNone(sql_stuff)
        self.maxDiff = None
        expected = "{}{}".format(expected_prefix, 'ORDER BY t1.east_lon LIMIT 10')
        self.assertMultiLineEqual(expected, sql_stuff)

    # def test_sort_by_bounds(self):
    #     expected_prefix = 'SELECT * FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] AS t1 '
    #     landsat_filters = LandsatQueryFilters()
    #     landsat_filters.bounds.sort_by(epl_imagery_pb2.ASCENDING)
    #     sql_stuff = landsat_filters.get_sql()
    #     self.assertIsNotNone(sql_stuff)
    #     self.maxDiff = None
    #     expected = "{}{}".format(expected_prefix, ' WHERE LIMIT 10')
    #     self.assertMultiLineEqual(expected, sql_stuff)
    #
    #     query_filter = landsat_filters.get_query_filter()
    #     new_landy = LandsatQueryFilters(query_filter=query_filter)
    #     self.assertEqual(epl_imagery_pb2.ASCENDING, new_landy.bounds.query_params.sort_direction)

    def test_satellite_id(self):
        landsat_filter = LandsatQueryFilters()
        landsat_filter.spacecraft_id.set_value(SpacecraftID.LANDSAT_4.name)
        sql_stuff = landsat_filter.get_sql()
        expected = "{}{}".format(expected_prefix, ' WHERE (t1.spacecraft_id IN ("LANDSAT_4")) LIMIT 10')
        self.assertMultiLineEqual(expected, sql_stuff)

        # don't allow repeat adds
        landsat_filter.spacecraft_id.set_value(SpacecraftID.LANDSAT_4.name)
        sql_stuff = landsat_filter.get_sql()
        self.assertMultiLineEqual(expected, sql_stuff)

    def test_geometry_vs_bounds(self):
        import shapely.wkb
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])
        landsat_filter = LandsatQueryFilters()
        landsat_filter.aoi.set_bounds(*area_shape.bounds)
        # with self.assertRaises(ValueError):
        #     landsat_filter.aoi.set_geometry(area_shape.wkb)

        landsat_filter = LandsatQueryFilters()
        landsat_filter.aoi.set_geometry(area_shape.wkb)
        # with self.assertRaises(ValueError):
        #     landsat_filter.aoi.set_bounds(*area_shape.bounds)

        self.maxDiff = None
        stuff = landsat_filter.aoi.get_geometry()
        self.assertMultiLineEqual(area_shape.wkt, shapely.wkb.loads(stuff[0]).wkt)

    def test_raise_sort_by(self):
        landsat_filter = LandsatQueryFilters()
        landsat_filter.wrs_path.sort_by(epl_imagery_pb2.DESCENDING)
        landsat_filter.aoi.sort_by(epl_imagery_pb2.ASCENDING)
        with self.assertRaises(NotImplementedError):
            landsat_filter.wrs_path_row.sort_by(epl_imagery_pb2.ASCENDING)

    def test_change_sort(self):
        landsat_filter = LandsatQueryFilters()

        landsat_filter.acquired.sort_by(epl_imagery_pb2.DESCENDING)
        sql_stuff = landsat_filter.get_sql()
        expected = expected_prefix + " ORDER BY t1.sensing_time DESC LIMIT 10"
        self.assertMultiLineEqual(expected, sql_stuff)
        query_filter = landsat_filter.get_query_filter()
        landsat_filter_2 = LandsatQueryFilters(query_filter=query_filter)
        self.assertMultiLineEqual(expected, landsat_filter_2.get_sql())

        self.assertEqual(landsat_filter_2.acquired.query_params.sort_direction, epl_imagery_pb2.DESCENDING)
        landsat_filter_2.wrs_row.sort_by(epl_imagery_pb2.DESCENDING)
        self.assertTrue(landsat_filter_2.acquired.query_params.sort_direction == epl_imagery_pb2.NOT_SORTED)
        landsat_filter.wrs_row.sort_by(epl_imagery_pb2.DESCENDING)

        sql_stuff = landsat_filter.get_sql()
        expected = expected_prefix + " ORDER BY t1.wrs_row DESC LIMIT 10"
        self.assertMultiLineEqual(expected, sql_stuff)
        query_filter = landsat_filter.get_query_filter()
        landsat_filter_2 = LandsatQueryFilters(query_filter=query_filter)
        self.assertMultiLineEqual(expected, landsat_filter_2.get_sql())

        landsat_filter_2.cloud_cover.sort_by(epl_imagery_pb2.ASCENDING)
        expected = expected_prefix + " ORDER BY t1.cloud_cover LIMIT 10"
        self.assertMultiLineEqual(expected, landsat_filter_2.get_sql())

        landsat_filter.aoi.sort_by(epl_imagery_pb2.DESCENDING)
        sql_stuff = landsat_filter.get_sql()
        expected = expected_prefix + " LIMIT 10"
        self.assertMultiLineEqual(expected, sql_stuff)
        query_filter = landsat_filter.get_query_filter()
        landsat_filter_2 = LandsatQueryFilters(query_filter=query_filter)
        self.assertMultiLineEqual(expected, landsat_filter_2.get_sql())
        self.assertEqual(landsat_filter_2.aoi.query_params.sort_direction, epl_imagery_pb2.DESCENDING)

    def test_clear_bounds(self):
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['collection_number!="PRE"']
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_exclude_value("PRE")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        expected = landsat_filter.get_sql()
        landsat_filter.aoi.set_bounds(*bounding_box)
        landsat_filter.aoi.query_params.ClearField("bounds")

        actual = landsat_filter.get_sql()
        self.maxDiff = None
        self.assertMultiLineEqual(expected, actual)

    def test_set_and_exclude(self):
        start_date = datetime.strptime('14295', '%y%j').date()
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_path.set_value(139)
        landsat_filters.wrs_row.set_value(45)
        landsat_filters.acquired.set_range(start_date, True, start_date, True)
        landsat_filters.collection_number.set_value("PRE")
        actual_1 = landsat_filters.get_sql()
        self.maxDiff = None
        expected = expected_prefix + ' WHERE (((((t1.sensing_time >= "2014-10-22T00:00:00") AND (t1.sensing_time <= "2014-10-22T23:59:59.999999")) AND (t1.collection_number IN ("PRE"))) AND (t1.wrs_path IN (139))) AND (t1.wrs_row IN (45))) LIMIT 10'
        self.assertMultiLineEqual(expected, actual_1)

        # # turn gernator into list
        # metadata_set = list(rows)
        # self.assertEqual(len(metadata_set), 1)
        # metadata = metadata_set[0]
        # self.assertEqual(metadata.get_aws_file_path(), "/imagery/L8/139/045/LC81390452014295LGN00")
        #
        landsat_filters.collection_number.set_exclude_value("PRE")
        actual_1 = landsat_filters.get_sql()
        expected = expected_prefix + ' WHERE (((((t1.sensing_time >= "2014-10-22T00:00:00") AND (t1.sensing_time <= "2014-10-22T23:59:59.999999")) AND NOT (t1.collection_number IN ("PRE"))) AND (t1.wrs_path IN (139))) AND (t1.wrs_row IN (45))) LIMIT 10'
        self.assertMultiLineEqual(expected, actual_1)
