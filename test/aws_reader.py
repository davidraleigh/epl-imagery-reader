import datetime
import unittest
import os
import json
import requests
import shapely.geometry
from datetime import date
from lxml import etree
from osgeo import gdal
from shapely.wkt import loads

from epl.grpc.imagery import epl_imagery_pb2
from epl.native.imagery import PLATFORM_PROVIDER
from epl.native.imagery.reader import MetadataService, SpacecraftID, Metadata, FunctionDetails, Landsat, DataType
from epl.native.imagery.metadata_helpers import LandsatQueryFilters
from test.tools.test_helpers import xml_compare


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
        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_path.set_value(139)
        landsat_filters.wrs_row.set_value(45)
        landsat_filters.acquired.set_range(start_date, True, start_date, True)
        landsat_filters.collection_number.set_value("PRE")
        rows = metadataservice.search(SpacecraftID.LANDSAT_8,
                                      data_filters=landsat_filters)

        # turn gernator into list
        metadata_set = list(rows)
        self.assertEqual(len(metadata_set), 1)
        metadata = metadata_set[0]
        self.assertEqual(metadata.get_aws_file_path(), "/imagery/L8/139/045/LC81390452014295LGN00")

        landsat_filters = LandsatQueryFilters()
        landsat_filters.wrs_path.set_value(139)
        landsat_filters.wrs_row.set_value(45)
        # landsat_filters.collection_number.set_exclude_value("PRE")
        landsat_filters.acquired.set_range(start=date(2017, 3, 4), end=date(2017, 3, 4))
        rows = metadataservice.search(SpacecraftID.LANDSAT_8,
                                      data_filters=landsat_filters)
        metadata_set = list(rows)
        self.assertEqual(len(metadata_set), 2)
        metadata = metadata_set[0]
        self.assertEqual(metadata.get_aws_file_path(),
                         "/imagery/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1")

        metadata = metadata_set[1]
        self.assertEqual(metadata.get_aws_file_path(),
                         "/imagery/L8/139/045/LC81390452017063LGN00")

    def test_google_aws_mismatch(self):
        metadata_service = MetadataService()
        d_start = date(2017, 6, 24)
        d_end = date(2017, 9, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_exclude_value("PRE")
        landsat_filters.acquired.set_range(start=d_start, end=d_end)
        landsat_filters.aoi.set_bounds(*bounding_box)
        rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                       limit=2,
                                       data_filters=landsat_filters)
        # generator to list
        rows = list(rows)
        metadata = rows[0]
        # self.assertEqual('LC08_L1GT_135215_20170916_20170916_01_RT', metadata.product_id)
        self.assertEqual('RT', metadata.collection_category)

    def test_aws_without_google(self):
        metadata_service = MetadataService()
        # c1/L8/083/015/LC08_L1TP_083015_20171106_20171107_01_RT/
        # c1/L8/083/111/LC08_L1GT_083111_20171106_20171107_01_RT

        sqs_message = {
            "Type": "Notification",
            "MessageId": "27f57c3d-9d2e-5fa3-8f83-2e41a3aa5634",
            "TopicArn": "arn:aws:sns:us-west-2:274514004127:NewSceneHTML",
            "Subject": "Amazon S3 Notification",
            "Message": "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-west-2\",\"eventTime\":\"2017-11-07T23:05:40.162Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAILHHXPNIKSGVUGOZK\"},\"requestParameters\":{\"sourceIPAddress\":\"35.193.238.175\"},\"responseElements\":{\"x-amz-request-id\":\"F96A6CC9816FC5EF\",\"x-amz-id-2\":\"yehs3XxTY8utc9kgKfNbMe1wdtV7F0wEMUXUQtIu7zMRtGvboxahzwncrmG046yI327j5IRh8nE=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"C1-NewHTML\",\"bucket\":{\"name\":\"landsat-pds\",\"ownerIdentity\":{\"principalId\":\"A3LZTVCZQ87CNW\"},\"arn\":\"arn:aws:s3:::landsat-pds\"},\"object\":{\"key\":\"c1/L8/115/062/LC08_L1TP_115062_20171107_20171107_01_RT/index.html\",\"size\":5391,\"eTag\":\"0f06667fca1f707894bf579bd667e221\",\"sequencer\":\"005A023C441A8F8403\"}}}]}",
            "Timestamp": "2017-11-07T23:05:40.219Z",
            "SignatureVersion": "1",
            "Signature": "bqrW1x6CgJntCz6f0F5uncyPZR+6ZM/tZ3OrRZDiudBv5DAtMyYR9n6KQ0aT+iYP5INfpL2GuIm8Uqco8ZHzg5AqEhHtNkpzGBQpQHvlF3t0ut9K27YNwJ6ZmnS14BgsLWyXIthVRjvHf1Hhx3ZInPMJrzTcKCOhOmBcM9zOpfWrHfnynuifpN3FaldDz6VY2d9QM0Rn8Fo8XZ4F+j01eAJVlydnRbSBbLewleuvhPQh6EG5r2EeekeniOIETrodS7o43ZClFr8OSgRE7BvpecVnnUEXBUIDDtRAPnIxo3Io0AmfPRI8xRfeKNhBIhPq3W3clm7Dxkp3N96OKoVUBw==",
            "SigningCertURL": "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-433026a4050d206028891664da859041.pem",
            "UnsubscribeURL": "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:274514004127:NewSceneHTML:7997d757-d1c6-4064-8935-34111968c8cc"
        }

        d = datetime.datetime.strptime(sqs_message['Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

        rows = metadata_service.search_aws('/imagery', wrs_path=115, wrs_row=62, collection_date=d)
        self.assertGreater(len(rows), 0)

        metadata = rows[0]
        self.assertIsNotNone(metadata)

        landsat = Landsat(metadata)
        nda = landsat.fetch_imagery_array([4, 3, 2], [[0, 40000], [0, 40000], [0, 40000]], spatial_resolution_m=240)
        self.assertIsNotNone(nda)

    def test_aws_from_image_key(self):
        sqs_message = {
            "Type": "Notification",
            "MessageId": "27f57c3d-9d2e-5fa3-8f83-2e41a3aa5634",
            "TopicArn": "arn:aws:sns:us-west-2:274514004127:NewSceneHTML",
            "Subject": "Amazon S3 Notification",
            "Message": "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-west-2\",\"eventTime\":\"2017-11-07T23:05:40.162Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAILHHXPNIKSGVUGOZK\"},\"requestParameters\":{\"sourceIPAddress\":\"35.193.238.175\"},\"responseElements\":{\"x-amz-request-id\":\"F96A6CC9816FC5EF\",\"x-amz-id-2\":\"yehs3XxTY8utc9kgKfNbMe1wdtV7F0wEMUXUQtIu7zMRtGvboxahzwncrmG046yI327j5IRh8nE=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"C1-NewHTML\",\"bucket\":{\"name\":\"landsat-pds\",\"ownerIdentity\":{\"principalId\":\"A3LZTVCZQ87CNW\"},\"arn\":\"arn:aws:s3:::landsat-pds\"},\"object\":{\"key\":\"c1/L8/115/062/LC08_L1TP_115062_20171107_20171107_01_RT/index.html\",\"size\":5391,\"eTag\":\"0f06667fca1f707894bf579bd667e221\",\"sequencer\":\"005A023C441A8F8403\"}}}]}",
            "Timestamp": "2017-11-07T23:05:40.219Z",
            "SignatureVersion": "1",
            "Signature": "bqrW1x6CgJntCz6f0F5uncyPZR+6ZM/tZ3OrRZDiudBv5DAtMyYR9n6KQ0aT+iYP5INfpL2GuIm8Uqco8ZHzg5AqEhHtNkpzGBQpQHvlF3t0ut9K27YNwJ6ZmnS14BgsLWyXIthVRjvHf1Hhx3ZInPMJrzTcKCOhOmBcM9zOpfWrHfnynuifpN3FaldDz6VY2d9QM0Rn8Fo8XZ4F+j01eAJVlydnRbSBbLewleuvhPQh6EG5r2EeekeniOIETrodS7o43ZClFr8OSgRE7BvpecVnnUEXBUIDDtRAPnIxo3Io0AmfPRI8xRfeKNhBIhPq3W3clm7Dxkp3N96OKoVUBw==",
            "SigningCertURL": "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-433026a4050d206028891664da859041.pem",
            "UnsubscribeURL": "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:274514004127:NewSceneHTML:7997d757-d1c6-4064-8935-34111968c8cc"
        }
        message_json = json.loads(sqs_message['Message'])
        image_key = message_json['Records'][0]['s3']['object']['key']
        path_name = '/imagery/' + os.path.dirname(image_key)
        # basename = os.path.basename(path_name)
        metadata = Metadata(path_name)
        self.assertEqual(115, metadata.wrs_path)
        self.assertEqual(62, metadata.wrs_row)

    def test_wrs_from_key_bug(self):
        failed = "/imagery/c1/L8/010/045/LC08_L1TP_010045_20171022_20171107_01_T1"
        metadata = failed
        self.assertIsNotNone(metadata)

    def test_cloud_cover(self):
        failed = "/imagery/c1/L8/020/035/LC08_L1TP_020035_20171028_20171108_01_T1"
        metadata = Metadata(failed)
        self.assertIsNotNone(metadata)
        metadata.parse_mtl("LC08_L1TP_020035_20171028_20171108_01_T1_MTL.json")
        self.assertIsNotNone(metadata.cloud_cover)

        self.assertIsNotNone(metadata.date_acquired)
        self.assertIsNotNone(metadata.sensing_time)

        self.assertEqual(metadata.sensing_time.date().isoformat(), metadata.date_acquired)
        self.assertNotEqual(metadata.date_acquired, metadata.date_processed.date())

    def test_gt(self):
        failed = "/imagery/c1/L8/137/208/LT08_L1GT_137208_20171117_20171117_01_RT"
        metadata = failed
        self.assertIsNotNone(metadata)

    # def test_metadata_service(self):
    #     metadata_service = MetadataService()
    #     sql_filters = ['cloud_cover=0']
    #     d_start = date(2015, 6, 24)
    #     d_end = date(2016, 6, 24)
    #     bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
    #     rows = metadata_service.search(
    #         SpacecraftID.LANDSAT_8,
    #         start_date=d_start,
    #         end_date=d_end,
    #         bounding_box=bounding_box,
    #         data_filters=landsat_filters)
    #
    #     self.assertEqual(len(rows), 10)
    #     for row in rows:
    #         self.assertEqual(row[2], SpacecraftID.LANDSAT_8.name)
    #         d_actual = datetime.datetime.strptime(row[4], '%Y-%m-%d').date()
    #         self.assertLessEqual(d_actual, d_end)
    #         self.assertGreaterEqual(d_actual, d_start)
    #         self.assertTrue(
    #             (bounding_box[0] < row[14] < bounding_box[2]) or (bounding_box[0] < row[15] < bounding_box[2]))
    #         self.assertTrue(
    #             (bounding_box[1] < row[12] < bounding_box[3]) or (bounding_box[1] < row[13] < bounding_box[3]))


class TestAWSPixelFunctions(unittest.TestCase):
    m_row_data = None
    base_mount_path = '/imagery'
    metadata_service = MetadataService()
    iowa_polygon = None
    metadata_set = []
    r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/NM/Taos.geo.json")
    taos_geom = r.json()
    taos_shape = shapely.geometry.shape(taos_geom['features'][0]['geometry'])

    def setUp(self):
        metadata_service = MetadataService()
        d_start = date(2015, 6, 24)
        d_end = date(2016, 6, 24)
        bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
        landsat_filters = LandsatQueryFilters()
        landsat_filters.scene_id.set_value("LC80400312016103LGN00")
        rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                       limit=1,
                                       data_filters=landsat_filters)
        rows = list(rows)
        self.m_row_data = rows[0]
        wkt_iowa = "POLYGON((-93.76075744628906 42.32707774458643,-93.47854614257812 42.32707774458643," \
                   "-93.47854614257812 42.12674735753131,-93.76075744628906 42.12674735753131," \
                   "-93.76075744628906 42.32707774458643))"
        self.iowa_polygon = loads(wkt_iowa)
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")

        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        landsat_filters = LandsatQueryFilters()
        landsat_filters.collection_number.set_exclude_value("PRE")
        landsat_filters.acquired.set_range(start=d_start, end=d_end)
        landsat_filters.aoi.set_bounds(*self.taos_shape.bounds)
        landsat_filters.data_type.set_exclude_value('L1GT')
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=10,
            data_filters=landsat_filters)

        for row in rows:
            self.metadata_set.append(row)

    def test_pixel_1(self):
        metadata = self.m_row_data
        landsat = Landsat(metadata)  # , gsurl[2])

        code = """import numpy as np
def multiply_rounded(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,
                   raster_ysize, buf_radius, gt, **kwargs):
    factor = float(kwargs['factor'])
    out_ar[:] = np.round_(np.clip(in_ar[0] * factor,0,255))"""

        function_arguments = {"factor": "1.5"}
        pixel_function_details = FunctionDetails(name="multiply_rounded", band_definitions=[2],
                                                 data_type=DataType.FLOAT32, code=code,
                                                 arguments=function_arguments)

        vrt = landsat.get_vrt([pixel_function_details, 3, 2])

        with open('pixel_1_aws.vrt', 'r') as myfile:
            data = myfile.read()
            expected = etree.XML(data)
            actual = etree.XML(vrt)
            result, message = xml_compare(expected, actual, {"GeoTransform": 1e-10})
            self.assertTrue(result, message)

    def test_missing_data(self):
        start = date(2017, 4, 15)
        end = date(2017, 5, 15)

        r = requests.get("https://raw.githubusercontent.com/johan/world.geo.json/master/countries/BEL.geo.json")

        area_geom = r.json()
        area_shape = shapely.geometry.shape(area_geom['features'][0]['geometry'])

        landsat_filter = LandsatQueryFilters()
        landsat_filter.acquired.set_range(start=start, end=end)
        landsat_filter.aoi.set_bounds(*area_shape.bounds)
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=40,
            data_filters=landsat_filter)

        metadataset= []
        for row in rows:
            metadataset.append(row)

        self.assertEqual(40, len(metadataset))

    def test_json_txt_mtl(self):
        failed_2 = "/imagery/c1/L8/089/078/LC08_L1TP_089078_20180612_20180613_01_RT"
        metadata2 = Metadata(failed_2)
        self.assertIsNotNone(metadata2)
        self.assertEqual(89, metadata2.wrs_path)
        self.assertEqual(78, metadata2.wrs_row)
        date(2018, 6, 12).isoformat()
        self.assertGreaterEqual(date(2018, 6, 12).isoformat(), metadata2.date_acquired)

    def test_missing_mtl_json(self):
        landsat_filter = LandsatQueryFilters()
        landsat_filter.product_id.set_value("LC08_L1TP_089078_20180612_20180613_01_RT")
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            limit=1,
            data_filters=landsat_filter)

        metadata_set = list(rows)
        self.assertEqual(1, len(metadata_set))

    def test_salt_lake_city(self):
        utah_wkt = 'POLYGON((-114.049883347847 38.677365, -114.049749 38.72920999999999, -114.049168 38.749951, -114.049465 38.874949, -114.048521 38.876197, -114.048054 38.878693, -114.049104 39.005509, -114.047079 39.49994299999999, -114.047727981839 39.5427408023268, -113.815766 39.54409, -113.815743 39.552644, -112.923426 39.552539, -112.803046 39.552648, -112.629539 39.5524, -112.462419 39.552451, -112.462423 39.553704, -112.354467 39.553684, -112.235958 39.553625, -112.212045 39.553987, -112.212029 39.54773, -112.207353 39.54769599999999, -112.207346 39.54408, -112.202672 39.544048, -112.202666 39.540434, -112.193292 39.540451, -112.193242 39.526023, -112.211973 39.526042, -112.211961 39.511579, -112.207285 39.51156599999999, -112.2073 39.504334, -112.202633 39.504323, -112.202626 39.489858, -112.193277 39.48983399999999, -112.193596 39.37309, -112.188907 39.373089, -112.188922 39.329392, -112.137293 39.329397, -112.10962 39.329522, -112.072108 39.329695, -112.072218 39.314923, -112.053423 39.31494199999999, -112.053421 39.314483, -112.016003 39.314561, -112.01452 39.132617, -112.014083 39.045518, -112.014017 39.024282, -112.019034 39.024207, -112.018906 38.995902, -112.056387 38.99568, -112.056402 38.988368, -112.065661 38.98843, -112.06563 38.985359, -112.065591 38.981689, -112.065162 38.959302, -112.133862 38.959302, -112.134031 38.935453, -112.151928 38.935473, -112.150513 38.92005899999999, -112.15076 38.906509, -112.169351 38.906132, -112.169461 38.892104, -112.171554 38.878616, -112.18847 38.879447, -112.188541 38.864787, -112.199098 38.864982, -112.206093 38.865112, -112.224461 38.86480299999999, -112.224553 38.85556, -112.224637 38.837514, -112.228438 38.837508, -112.228381 38.763987, -112.219118 38.763987, -112.219116 38.751251, -112.21808 38.748987, -112.218147 38.740198, -112.218356 38.72736099999999, -112.23664 38.727104, -112.236685 38.723266, -112.236722 38.719859, -112.241362 38.719785, -112.24147 38.71252399999999, -112.255359 38.712306, -112.255438 38.705047, -112.273297 38.704999, -112.273255 38.701353, -112.28235 38.701527, -112.28226 38.686874, -112.291316 38.686961, -112.291329 38.683298, -112.30994 38.68340999999999, -112.309953 38.67629, -112.347179 38.676259, -112.356637 38.67674, -112.356539 38.683855, -112.365964 38.684218, -112.384245 38.68331, -112.401176 38.680972, -112.4198 38.681388, -112.447517 38.68127, -112.447667 38.67392299999999, -112.466001 38.673917, -112.466156 38.659267, -112.475341 38.659257, -112.475185 38.644675, -112.484444 38.644647, -112.484788 38.600632, -112.503074 38.600568, -112.503148 38.571377, -112.51239 38.571358, -112.515394 38.572845, -112.603479 38.572733, -112.752497 38.572681, -113.047215 38.572603, -113.115956 38.572663, -113.11686 38.572612, -113.189536 38.57273199999999, -113.191158 38.57261099999999, -114.05015385888 38.5729744583009, -114.049883347847 38.677365))'
        utah_shape = shapely.wkt.loads(utah_wkt)
        landsat_qf = LandsatQueryFilters()
        # cloud cover less than 30%
        landsat_qf.cloud_cover.set_range(end=30)
        landsat_qf.aoi.set_geometry(utah_shape.wkb)
        # sort by date, with most recent first
        landsat_qf.acquired.sort_by(epl_imagery_pb2.DESCENDING)

        metadata_servce = MetadataService()
        rows = metadata_servce.search_layer_group(data_filters=landsat_qf,
                                                  satellite_id=SpacecraftID.LANDSAT_8)
        self.assertIsNotNone(rows)
        metadata_set = list(rows)
        self.assertGreaterEqual(len(metadata_set), 1)

