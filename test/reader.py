import unittest
import datetime

import requests
import shapely.geometry

import numpy as np

from shapely.geometry import shape
from shapely.geometry import box
from shapely.wkt import loads
from google.cloud import bigquery

from datetime import date
from epl.native.imagery.reader import MetadataService, Landsat, Metadata, WRSGeometries, DataType
from epl.native.imagery.metadata_helpers import LandsatQueryFilters, SpacecraftID, BandMap, Band
from epl.grpc.geometry.geometry_operators_pb2 import GeometryBagData
from epl.grpc.imagery import epl_imagery_pb2


class TestMetaDataSQL(unittest.TestCase):
    def test_peewee_1(self):
        a = LandsatQueryFilters()
        a.scene_id.set_value("LC80270312016188LGN00")
        sql_stuff = a.get_sql()

        client = bigquery.Client()
        query = client.run_sync_query(sql_stuff)
        query.timeout_ms = 1000
        query.run()

        for row in query.rows:
            metadata = Metadata(row)
            self.assertEqual(metadata.scene_id, "LC80270312016188LGN00")

    def test_polygon_boundary(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
        taos_geom = r.json()
        taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])
        metadata_service = MetadataService()

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(d_start, True, d_end, True)
        landsat_filters.aoi.set_geometry(taos_shape.wkb)
        # landsat_filters.geometry_bag.geometry_binaries.append(taos_shape.wkb)

        metadata_rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=landsat_filters)

        # mounted directory in docker container
        metadata_set = []

        for row in metadata_rows:
            metadata_set.append(row)

        self.assertEqual(len(metadata_set), 2)

    def test_where_start(self):
        # sql_filters = ['scene_id="LC80270312016188LGN00"']
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80270312016188LGN00")
        metadata_service = MetadataService()
        metadata_rows = metadata_service.search(
            SpacecraftID.UNKNOWN_SPACECRAFT,
            data_filters=landsat_filters)

        metadata_set = list(metadata_rows)
        self.assertEqual(1, len(metadata_set))
        # landsat = Landsat(metadata_set)
        # data = landsat.fetch_imagery_array(
        #     band_definitions=[Band.RED, Band.GREEN, Band.BLUE, Band.ALPHA],
        #     spatial_resolution_m=960)
        #
        # self.assertEqual(data.shape, (249, 245, 4))
        # self.assertEqual(data.dtype, np.uint8)

    def test_scene_id(self):
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80390332016208LGN00")
        metadata_service = MetadataService()
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, data_filters=landsat_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 1)

    def test_start_date(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        landsat_filters = LandsatQueryFilters()
        landsat_filters.acquired.set_range(d, True)
        landsat_filters.wrs_path.set_value(125)
        landsat_filters.wrs_row.set_value(49)
        landsat_filters.acquired.sort_by(epl_imagery_pb2.ASCENDING)
        landsat_filters.acquired.set_range(end=d, end_inclusive=True)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, data_filters=landsat_filters)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        d_previous = datetime.datetime.strptime("1945-01-01", '%Y-%m-%d').date()
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()

            # test Order by
            self.assertGreaterEqual(d_actual, d_previous)
            d_previous = d_actual

    def test_end_date(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.acquired.set_range(end=d, end_inclusive=True)
        landsat_filter.acquired.sort_by(epl_imagery_pb2.DESCENDING)
        landsat_filter.aoi.set_bounds(*area_shape.bounds)
        rows = metadata_service.search(SpacecraftID.LANDSAT_7, data_filters=landsat_filter)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        d_previous = d
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_7.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_previous)

            d_previous = d_actual

    def test_one_day(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d = date(2016, 6, 24)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.acquired.set_value(d)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                       data_filters=landsat_filter)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertEqual(d_actual, d)

    def test_1_year(self):
        # gs://gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, data_filters=landsat_filter)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)

    def test_bounding_box_1(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.aoi.set_bounds(*bounding_box)
        metadata_rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                                data_filters=landsat_filter)

        metadata_rows = list(metadata_rows)

        self.assertEqual(len(metadata_rows), 10)
        for row in metadata_rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            test_box = row.bounds
            self.assertTrue(
                (bounding_box[0] < test_box[2] < bounding_box[2]) or
                (bounding_box[0] < test_box[0] < bounding_box[2]))
            self.assertTrue(
                (bounding_box[1] < test_box[3] < bounding_box[3]) or
                (bounding_box[1] < test_box[1] < bounding_box[3]))

    def test_cloud_cover(self):
        metadata_service = MetadataService()
        landsat_filters = LandsatQueryFilters()
        landsat_filters.cloud_cover.set_value(0)
        # landsat_filters.scene_id.set_value("LC80390332016208LGN00")
        # sql_filters = ['cloud_cover=0']
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        landsat_filters.acquired.set_range(d_start, True, d_end, True)
        landsat_filters.aoi.set_bounds(*bounding_box)
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            data_filters=landsat_filters)

        rows = list(rows)

        self.assertEqual(len(rows), 10)
        for row in rows:
            self.assertEqual(row.spacecraft_id.name, SpacecraftID.LANDSAT_8.name)
            d_actual = datetime.datetime.strptime(row.date_acquired, '%Y-%m-%d').date()
            self.assertLessEqual(d_actual, d_end)
            self.assertGreaterEqual(d_actual, d_start)
            test_box = row.bounds
            self.assertTrue(
                (bounding_box[0] < test_box[2] < bounding_box[2]) or
                (bounding_box[0] < test_box[0] < bounding_box[2]))
            self.assertTrue(
                (bounding_box[1] < test_box[3] < bounding_box[3]) or
                (bounding_box[1] < test_box[1] < bounding_box[3]))

    def test_metadata_singleton(self):
        metadata_service_1 = MetadataService()
        metadata_service_2 = MetadataService()
        self.assertTrue(metadata_service_1 is metadata_service_2)

    def test_colorado(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/CO/Boulder.geo.json")
        boulder_geom = r.json()
        boulder_shape = shapely.geometry.shape(boulder_geom['features'][0]['geometry'])

        d_start = date(2017, 3, 1)  # 2017-03-12
        d_end = date(2017, 3, 19)  # epl api is inclusive

        # PRE is a collection type that specifies certain QA standards
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_value("PRE")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.aoi.set_bounds(*boulder_shape.bounds)

        # search the satellite metadata for images of Taos withing the given date range
        metadata_service = MetadataService()
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=landsat_filter)

        self.assertTrue(2, len(list(rows)))

    def test_split_1(self):
        wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((-172 53, 175 53, 175 48, -172 48, -172 53)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        islands_shape = loads(wkt)

        with self.assertRaises(ValueError):
            MetadataService.split_by_dateline(islands_shape)

    def test_split_2(self):
        wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        islands_shape = loads(wkt)

        for geom in islands_shape.geoms:
            self.assertIsNotNone(MetadataService.split_by_dateline(geom))

    def test_split_3(self):
        wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((-172 53, 175 53, 175 48, -172 48, -172 53)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        islands_shape = loads(wkt)

        result = MetadataService._dateline_intersection((172, 53), (-172, 53))
        self.assertIsNotNone(result)
        self.assertEqual((180, 53), result[0])
        self.assertEqual((-180, 53), result[1])
        result = MetadataService._dateline_intersection((-172, 53), (172, 53))
        self.assertIsNotNone(result)
        self.assertEqual((180, 53), result[1])
        self.assertEqual((-180, 53), result[0])

        result = MetadataService._dateline_intersection((-172, 55), (172, 53))
        self.assertIsNotNone(result)
        self.assertEqual((180, 54), result[1])
        self.assertEqual((-180, 54), result[0])

        result = MetadataService._dateline_intersection((172, 53), (-172, -53))
        self.assertIsNotNone(result)
        self.assertEqual((180, 0), result[0])
        self.assertEqual((-180, 0), result[1])

        result = MetadataService._dateline_intersection((172, 53), (172, -53))
        self.assertIsNone(result)

        result = MetadataService._dateline_intersection((-172, 53), (-172, -53))
        self.assertIsNone(result)

        for geom in islands_shape.geoms:
            # self.assertFalse(geom.exterior.is_ccw)
            clean_poly = MetadataService.split_by_dateline(geom)
            self.assertIsNotNone(clean_poly)
            if clean_poly:
                for geom_result in clean_poly:
                    self.assertTrue(geom_result.exterior.is_ccw)

        results = MetadataService.split_all_by_dateline([islands_shape.wkb])
        for geom_result in results:
            self.assertIsNotNone(geom_result)
            self.assertTrue(geom_result.exterior.is_ccw)

    def test_split_wrs(self):
        wkt = "POLYGON((-172 53, 175 53, 175 48, -172 48, -172 53))"
        islands_shape = loads(wkt)
        metadata_service = MetadataService()
        wrs_set = metadata_service.get_wrs([islands_shape.wkb], search_area_unioned=islands_shape)
        sorted_overlaps = metadata_service.sorted_wrs_overlaps(wrs_set=wrs_set, search_area=islands_shape)
        self.assertIsNotNone(wrs_set)
        self.assertIsNotNone(sorted_overlaps)
        self.assertGreater(len(wrs_set), 0)
        self.assertGreater(len(sorted_overlaps), 0)
        prev_area = 0
        for wrs_overlap in sorted_overlaps:
            self.assertLessEqual(prev_area, wrs_overlap[2])
            prev_area = wrs_overlap[2]

    def test_alaskan_aleutians(self):
        # wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((-172 53, 175 53, 175 48, -172 48, -172 53)), ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20)))"
        wkt = "POLYGON((-172 53, 175 53, 175 48, -172 48, -172 53))"
        islands_shape = loads(wkt)
        self.assertFalse(islands_shape.exterior.is_ccw)
        query_filter = LandsatQueryFilters()
        query_filter.aoi.set_geometry(islands_shape.wkb)
        # query_filter.geometry_bag.geometry_binaries.append(islands_shape.wkb)
        metadata_service = MetadataService()
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=query_filter)

        stuff = list(rows)
        self.assertEqual(10, len(stuff))

    def test_belgium(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])

        d_start = date(2017, 1, 1)  # 2017-03-12
        d_end = date(2017, 5, 19)  # epl api is inclusive

        belgium_filter = LandsatQueryFilters()

        # PRE is a collection type that specifies certain QA standards
        belgium_filter.collection_number.set_value("PRE")
        belgium_filter.cloud_cover.set_range(end=15, end_inclusive=False)
        belgium_filter.acquired.set_range(start=d_start, end=d_end)
        belgium_filter.aoi.set_bounds(*area_shape.bounds)
        # search the satellite metadata for images of Belgium withing the given date range
        metadata_service = MetadataService()
        rows = metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=20,
            data_filters=belgium_filter)

    def test_search_group(self):
        import shapely.wkb
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])

        d_start = date(2017, 1, 1)  # 2017-03-12
        d_end = date(2017, 5, 19)  # epl api is inclusive

        belgium_filter = LandsatQueryFilters()

        # PRE is a collection type that specifies certain QA standards
        # belgium_filter.collection_number.set_value("PRE")
        belgium_filter.cloud_cover.set_range(end=15, end_inclusive=False)
        belgium_filter.acquired.set_range(start=d_start, end=d_end)
        belgium_filter.aoi.set_bounds(*area_shape.bounds)
        # search the satellite metadata for images of Belgium withing the given date range
        metadata_service = MetadataService()
        metadata_gen = metadata_service.search_layer_group(data_filters=belgium_filter, satellite_id=SpacecraftID.LANDSAT_8)
        unioned_beast = shapely.geometry.Polygon()
        for metadata in metadata_gen:
            wrs_polygon = metadata.get_wrs_polygon()
            wrs_shape = shapely.wkb.loads(wrs_polygon)
            unioned_beast = unioned_beast.union(wrs_shape)

        self.assertTrue(unioned_beast.contains(area_shape))

        belgium_filter.aoi.sort_by(epl_imagery_pb2.DESCENDING)
        metadata_gen = metadata_service.search_layer_group(data_filters=belgium_filter,
                                                           satellite_id=SpacecraftID.LANDSAT_8)
        unioned_beast = shapely.geometry.Polygon()
        for metadata in metadata_gen:
            wrs_polygon = metadata.get_wrs_polygon()
            wrs_shape = shapely.wkb.loads(wrs_polygon)
            unioned_beast = unioned_beast.union(wrs_shape)

        self.assertTrue(unioned_beast.contains(area_shape))


class TestMetadata(unittest.TestCase):
    def test_bounds(self):
        row = ('LC80330352017072LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2017-03-13', '2017-03-13T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        geom_wkb = metadata.get_wrs_polygon()
        self.assertIsNotNone(geom_wkb)
        bounding_polygon = box(*metadata.bounds)
        wrs_polygon = shapely.wkb.loads(geom_wkb)
        self.assertTrue(bounding_polygon.contains(wrs_polygon))

        # polygon = loads(wkt)
        # self.assertEqual(polygon.wkt, wkt)
        # self.assertEqual(polygon.bounds, metadata.bounds)
        # self.assertTrue(True)

    def test_interesct(self):
        self.assertTrue(True)

    def test_epsg_codes(self):
        self.assertEqual(32601, Metadata.get_utm_epsg_code(-180, 45))
        self.assertEqual(32701, Metadata.get_utm_epsg_code(-180, -45))
        self.assertEqual(32601, Metadata.get_utm_epsg_code(-174, 45))
        self.assertEqual(32701, Metadata.get_utm_epsg_code(-174, -45))

        self.assertEqual(32602, Metadata.get_utm_epsg_code(-173.99, 45))
        self.assertEqual(32702, Metadata.get_utm_epsg_code(-173.99, -45))
        self.assertEqual(32602, Metadata.get_utm_epsg_code(-168, 45))
        self.assertEqual(32702, Metadata.get_utm_epsg_code(-168, -45))

        self.assertEqual(32603, Metadata.get_utm_epsg_code(-167.99, 45))
        self.assertEqual(32703, Metadata.get_utm_epsg_code(-167.99, -45))
        self.assertEqual(32603, Metadata.get_utm_epsg_code(-162, 45))
        self.assertEqual(32703, Metadata.get_utm_epsg_code(-162, -45))

        self.assertEqual(32660, Metadata.get_utm_epsg_code(180, 45))
        self.assertEqual(32760, Metadata.get_utm_epsg_code(180, -45))
        self.assertEqual(32660, Metadata.get_utm_epsg_code(174.00001, 45))
        self.assertEqual(32760, Metadata.get_utm_epsg_code(174.00001, -45))

        self.assertEqual(32659, Metadata.get_utm_epsg_code(174, 45))
        self.assertEqual(32759, Metadata.get_utm_epsg_code(174, -45))
        self.assertEqual(32659, Metadata.get_utm_epsg_code(168.00001, 45))
        self.assertEqual(32759, Metadata.get_utm_epsg_code(168.00001, -45))

    def test_utm_epsg(self):
        row = ('LC80330352017072LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2017-03-13', '2017-03-13T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        self.assertEqual(32613, metadata.utm_epsg_code)

    def test_doy(self):
        row = ('LC80330352017072LGN00_FAKED', '', 'LANDSAT_8', 'OLI_TIRS', '2017-11-04', '2017-11-04T17:38:14.0196140Z',
               'PRE', 'N/A', 'L1T', 33, 35, 1.2, 37.10422, 34.96178, -106.85883, -104.24596, 1067621299,
               'gs://gcp-public-data-landsat/LC08/PRE/033/035/LC80330352017072LGN00_FAKE')
        metadata = Metadata(row)
        self.assertIsNotNone(metadata)
        self.assertEqual(308, metadata.doy)


class TestBandMap(unittest.TestCase):
    def test_landsat_5(self):
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertEqual(band_map.get_number(Band.BLUE), 1)
        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_number(Band.THERMAL), 6)

        self.assertEqual(band_map.get_band_enum(1), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)
        self.assertEqual(band_map.get_band_enum(6), Band.THERMAL)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 1), val)
            self.assertEqual(band_map.get_number(val), idx + 1)
            self.assertEqual(band_map.get_resolution(val), 30.0)

    def test_landsat_5_exceptions(self):
        band_map_2 = BandMap(SpacecraftID.LANDSAT_7)
        self.assertEqual(band_map_2.get_number(Band.PANCHROMATIC), 8)
        self.assertEqual(band_map_2.get_band_enum(8), Band.PANCHROMATIC)
        self.assertEqual(band_map_2.get_resolution(Band.PANCHROMATIC), 15.0)
        band_map = BandMap(SpacecraftID.LANDSAT_5)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.CIRRUS))
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.PANCHROMATIC))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(8))

    def test_landsat_7(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertEqual(band_map.get_number(Band.BLUE), 1)
        self.assertEqual(band_map.get_number(Band.SWIR1), 5)
        self.assertEqual(band_map.get_number(Band.THERMAL), 6)

        self.assertEqual(band_map.get_number(Band.PANCHROMATIC), 8)
        self.assertEqual(band_map.get_band_enum(8), Band.PANCHROMATIC)
        self.assertEqual(band_map.get_resolution(Band.PANCHROMATIC), 15.0)

        self.assertEqual(band_map.get_band_enum(1), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(5), Band.SWIR1)
        self.assertEqual(band_map.get_band_enum(6), Band.THERMAL)
        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 1), val)
            self.assertEqual(band_map.get_number(val), idx + 1)
            self.assertEqual(band_map.get_resolution(val), 30.0)

    def test_landsat_7_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_7)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.CIRRUS))
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.TIRS1))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(9))

    def test_landsat_8(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertEqual(band_map.get_number(Band.ULTRA_BLUE), 1)
        self.assertEqual(band_map.get_number(Band.BLUE), 2)
        self.assertEqual(band_map.get_number(Band.SWIR1), 6)

        self.assertEqual(band_map.get_band_enum(2), Band.BLUE)
        self.assertEqual(band_map.get_band_enum(6), Band.SWIR1)

        self.assertEqual(band_map.get_number(Band.SWIR2), 7)
        self.assertEqual(band_map.get_band_enum(7), Band.SWIR2)

        for idx, val in enumerate([Band.BLUE, Band.GREEN, Band.RED, Band.NIR, Band.SWIR1]):
            self.assertEqual(band_map.get_band_enum(idx + 2), val)
            self.assertEqual(band_map.get_number(val), idx + 2)

        self.assertEqual(band_map.get_number(Band.CIRRUS), 9)
        self.assertEqual(band_map.get_number(Band.TIRS1), 10)
        self.assertEqual(band_map.get_number(Band.TIRS2), 11)
        self.assertEqual(band_map.get_resolution(Band.PANCHROMATIC), 15.0)

        self.assertEqual(band_map.get_band_enum(9), Band.CIRRUS)
        self.assertEqual(band_map.get_band_enum(10), Band.TIRS1)
        self.assertEqual(band_map.get_band_enum(11), Band.TIRS2)

    def test_landsat_8_exceptions(self):
        band_map = BandMap(SpacecraftID.LANDSAT_8)
        self.assertRaises(KeyError, lambda: band_map.get_number(Band.THERMAL))
        self.assertRaises(KeyError, lambda: band_map.get_band_enum(12))


class TestDataType(unittest.TestCase):
    def test_bitwise(self):
        a = DataType.UINT32
        b = DataType.UINT16


class TestWRSGeometries(unittest.TestCase):
    test_cases = [[15.74326, 26.98611, 1, 1, 1, 0, 13001, 13001, 13, 1, 'D', 1, 2233],
                  [2.74362, 6.65058, 942, 942, 1, 0, 61198, 61198, 61, 198, 'A', 1, 3174],
                  [13.37321, 24.20422, 2225, 2225, 1, 0, 125241, 125241, 125, 241, 'A', 2, 4209],
                  [3.58953, 7.7865, 1021, 1021, 1, 0, 75029, 75029, 75, 29, 'D', 3, 10445],
                  [4.2424, 8.69453, 891, 891, 1, 0, 64147, 64147, 64, 147, 'A', 6, 21227],
                  [16.81754, 27.20801, 3720, 3720, 1, 0, 223248, 223248, 223, 248, 'D', 16, 56296]]
    wrs_geometries = WRSGeometries()

    def test_geometry(self):
        for test_case in self.test_cases:
            geom_wkb = self.wrs_geometries.get_wrs_geometry(test_case[8], test_case[9])
            geom_expected_area = test_case[0]

            self.assertIsNotNone(geom_wkb)
            s = shapely.wkb.loads(geom_wkb)
            self.assertAlmostEqual(geom_expected_area, s.area, 5)

    def test_belgium(self):
        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])

        bounds_set = None
        while bounds_set is None:
            bounds_set = self.wrs_geometries.get_path_row((2.513573, 49.529484, 6.156658, 51.475024))

        for path_row in bounds_set:
            geom_wkb = self.wrs_geometries.get_wrs_geometry(path_row[0], path_row[1])
            s = shapely.wkb.loads(geom_wkb)
            b_intersect = s.envelope.intersects(area_shape.envelope)
            self.assertTrue(b_intersect)

        # filehandler = open("/.epl/wrs_geom.obj", "wb")
        # import pickle
        # pickle.dump(self.wrs_geometries, filehandler)
        # filehandler.close()

    def test_bounds_search(self):
        for idx, test_case in enumerate(self.test_cases):
            geom_wkb = self.wrs_geometries.get_wrs_geometry(test_case[8], test_case[9])
            original_shape = shapely.wkb.loads(geom_wkb)
            result = self.wrs_geometries.get_path_row(original_shape.bounds)
            path_pair = result.pop()
            while path_pair is not None:
                geom_wkb = self.wrs_geometries.get_wrs_geometry(path_pair[0], path_pair[1])
                s = shapely.wkb.loads(geom_wkb)
                b_intersect = s.envelope.intersects(original_shape.envelope)
                if not b_intersect:
                    print("Test case {0}\n original bounds: {1}\nnon-intersecting bounds{2}\n".format(idx,
                                                                                                      original_shape.bounds,
                                                                                                      s.bounds))

                self.assertTrue(b_intersect, "Test case {0}\n original bounds: {1}\nnon-intersecting bounds{2}\n"
                                .format(idx, original_shape.bounds, s.bounds))
                if result:
                    path_pair = result.pop()
                else:
                    break


class TestLandsat(unittest.TestCase):
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

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(d_start, True, d_end, True)
        landsat_filters.aoi.set_bounds(*self.taos_shape.bounds)
        metadata_rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=landsat_filters)

        # mounted directory in docker container
        base_mount_path = '/imagery'

        for row in metadata_rows:
            self.metadata_set.append(row)

    # TODO test PRE rejection
    # TODO test date range rejection
    # TODO test Satellite Rejection

    def test_vrt_not_pre(self):
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        # sql_filters = ['collection_number!="PRE"']
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_exclude_value("PRE")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.aoi.set_bounds(*bounding_box)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            limit=1,
                                            data_filters=landsat_filter)

        rows = list(rows)
        metadata = rows[0]

        landsat = Landsat(metadata)
        self.assertIsNotNone(landsat)
        vrt = landsat.get_vrt([4, 3, 2])
        self.assertIsNotNone(vrt)
        dataset = landsat.get_dataset([4, 3, 2], DataType.UINT16)
        self.assertIsNotNone(dataset)

    def test_pixel_function_vrt_1(self):
        utah_box = (-112.66342163085938, 37.738141282210385, -111.79824829101562, 38.44821130413263)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)

        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_value("PRE")
        landsat_filter.cloud_cover.set_range(end=5, end_inclusive=True) #landsat_filter.cloud_cover.set_range_end(5, True)
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        landsat_filter.aoi.set_bounds(*utah_box)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            limit=10,
                                            data_filters=landsat_filter)
        rows = list(rows)
        self.assertEqual(0, len(rows))

        d_end = date(2016, 8, 28)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.collection_number.set_value("PRE")
        landsat_filter.cloud_cover.set_range(end=5, end_inclusive=False) #landsat_filter.cloud_cover.set_range_end(5, False)
        landsat_filter.acquired.set_range(end=d_end, end_inclusive=True) #landsat_filter.acquired.set_range_end(d_end, True)
        landsat_filter.acquired.sort_by(epl_imagery_pb2.DESCENDING)
        landsat_filter.aoi.set_bounds(*utah_box)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            limit=10,
                                            data_filters=landsat_filter)
        rows = list(rows)
        self.assertEqual(len(rows), 10)
        #     metadata_row = ['LC80390332016208LGN00', '', 'LANDSAT_8', 'OLI_TIRS', '2016-07-26',
        # '2016-07-26T18:14:46.9465460Z', 'PRE', 'N/A', 'L1T', 39, 33, 1.69,
        # 39.96962, 37.81744, -115.27267, -112.56732, 1070517542,
        # 'gs://gcp-public-data-landsat/LC08/PRE/039/033/LC80390332016208LGN00']
        metadata = rows[0]

        # GDAL helper functions for generating VRT
        landsat = Landsat([metadata])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [4, 3, 2]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params)

        self.assertEqual(nda.shape, (3876, 3806, 3))

    def test_band_enum(self):
        self.assertTrue(True)
        d_start = date(2016, 7, 20)
        d_end = date(2016, 7, 28)
        landsat_filter = LandsatQueryFilters()
        landsat_filter.scene_id.set_value("LC80390332016208LGN00")
        landsat_filter.acquired.set_range(d_start, True, d_end, True)
        rows = self.metadata_service.search(SpacecraftID.LANDSAT_8,
                                            limit=1,
                                            data_filters=landsat_filter)
        rows = list(rows)
        metadata = rows[0]
        landsat = Landsat(metadata)
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        # nda = landsat.__get_ndarray(band_numbers, metadata, scale_params)
        nda = landsat.fetch_imagery_array([Band.RED, Band.GREEN, Band.BLUE], scale_params, spatial_resolution_m=240)
        self.assertIsNotNone(nda)
        nda2 = landsat.fetch_imagery_array([4, 3, 2], scale_params, spatial_resolution_m=240)
        np.testing.assert_almost_equal(nda, nda2)
        # 'scene_id': 'LC80390332016208LGN00'

    def test_vrt_extent(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set[0])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        vrt = landsat.get_vrt(band_numbers, envelope_boundary=self.taos_shape.bounds)

        self.assertIsNotNone(vrt)

    def test_cutline(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set[0])

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params, self.taos_shape.wkb, spatial_resolution_m=480)
        self.assertIsNotNone(nda)

        # TODO needs shape test

    def test_mosaic(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scale_params = [[0.0, 65535], [0.0, 65535], [0.0, 65535]]
        nda = landsat.fetch_imagery_array(band_numbers, scale_params, envelope_boundary=self.taos_shape.bounds)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

        # TODO needs shape test

    def test_mosaic_cutline(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

    def test_polygon_wkb_metadata(self):
        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        self.metadata_service = MetadataService()

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_value("PRE")
        landsat_filters.acquired.set_range(d_start, True, d_end, True)
        landsat_filters.aoi.set_geometry(self.taos_shape.wkb)
        # landsat_filters.geometry_bag.geometry_binaries.append(self.taos_shape.wkb)
        metadata_rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=landsat_filters)

        metadata_set = []
        for row in metadata_rows:
            metadata_set.append(row)

        landsat = Landsat(metadata_set)
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

    def test_mosaic_mem_error(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, envelope_boundary=self.taos_shape.bounds)

        self.assertIsNotNone(nda)
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)
        self.assertEqual((1804, 1295, 3), nda.shape)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb)
        self.assertIsNotNone(nda)
        self.assertEqual((1804, 1295, 3), nda.shape)

    def test_datatypes(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]

        for data_type in DataType:
            if data_type is DataType.UNKNOWN_GDAL:
                continue

            nda = landsat.fetch_imagery_array(band_numbers,
                                              scaleParams,
                                              envelope_boundary=self.taos_shape.bounds,
                                              output_type=data_type,
                                              spatial_resolution_m=240)
            self.assertIsNotNone(nda)
            self.assertGreaterEqual(data_type.range_max, nda.max())
            self.assertLessEqual(data_type.range_min, nda.min())

    def test_vrt_with_alpha(self):
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        band_numbers = [Band.RED, Band.GREEN, Band.BLUE, Band.ALPHA]
        scaleParams = [[0.0, 40000], [0.0, 40000], [0.0, 40000]]

        nda = landsat.fetch_imagery_array(band_numbers,
                                          scaleParams,
                                          envelope_boundary=self.taos_shape.bounds,
                                          output_type=DataType.UINT16,
                                          spatial_resolution_m=120)
        self.assertIsNotNone(nda)

    def test_rastermetadata_cache(self):
        # GDAL helper functions for generating VRT
        landsat = Landsat(self.metadata_set)

        # get a numpy.ndarray from bands for specified imagery
        # 'nir', 'swir1', 'swir2'
        band_numbers = [Band.NIR, Band.SWIR1, Band.SWIR2]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0], [0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb, spatial_resolution_m=120)
        self.assertIsNotNone(nda)
        self.assertEqual((902, 648, 3), nda.shape)

        band_numbers = [Band.RED, Band.BLUE, Band.GREEN]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb, spatial_resolution_m=120)
        self.assertIsNotNone(nda)
        self.assertEqual((902, 648, 3), nda.shape)

        band_numbers = [Band.RED, Band.BLUE, Band.GREEN]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, spatial_resolution_m=120)
        self.assertIsNotNone(nda)
        self.assertNotEqual((902, 648, 3), nda.shape)

    def test_two_bands(self):
        # specify the bands that approximate real color
        landsat = Landsat(self.metadata_set)
        band_numbers = [Band.RED, Band.BLUE]
        scaleParams = [[0.0, 40000.0], [0.0, 40000.0]]

        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb, spatial_resolution_m=120)
        self.assertIsNotNone(nda)
        self.assertEqual((2, 902, 648), nda.shape)

    def test_one_band(self):
        # specify the bands that approximate real color
        landsat = Landsat(self.metadata_set)
        band_numbers = [Band.RED]
        scaleParams = [[0.0, 40000.0]]
        nda = landsat.fetch_imagery_array(band_numbers, scaleParams, polygon_boundary_wkb=self.taos_shape.wkb,
                                          spatial_resolution_m=120)
        self.assertIsNotNone(nda)
        self.assertEqual((902, 648), nda.shape)
