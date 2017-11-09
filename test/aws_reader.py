import datetime
import unittest
from datetime import date

import json
import requests
import shapely.geometry
from lxml import etree
from osgeo import gdal
from shapely.wkt import loads

from epl.imagery import PLATFORM_PROVIDER
from epl.imagery.reader import MetadataService, SpacecraftID, Metadata, FunctionDetails, Landsat, DataType
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

    def test_aws_without_google(self):
        metadata_service = MetadataService()
        # c1/L8/083/015/LC08_L1TP_083015_20171106_20171107_01_RT/
        # c1/L8/083/111/LC08_L1GT_083111_20171106_20171107_01_RT

        sqs_message = {
            "Type" : "Notification",
            "MessageId" : "27f57c3d-9d2e-5fa3-8f83-2e41a3aa5634",
            "TopicArn" : "arn:aws:sns:us-west-2:274514004127:NewSceneHTML",
            "Subject" : "Amazon S3 Notification",
            "Message" : "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-west-2\",\"eventTime\":\"2017-11-07T23:05:40.162Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAILHHXPNIKSGVUGOZK\"},\"requestParameters\":{\"sourceIPAddress\":\"35.193.238.175\"},\"responseElements\":{\"x-amz-request-id\":\"F96A6CC9816FC5EF\",\"x-amz-id-2\":\"yehs3XxTY8utc9kgKfNbMe1wdtV7F0wEMUXUQtIu7zMRtGvboxahzwncrmG046yI327j5IRh8nE=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"C1-NewHTML\",\"bucket\":{\"name\":\"landsat-pds\",\"ownerIdentity\":{\"principalId\":\"A3LZTVCZQ87CNW\"},\"arn\":\"arn:aws:s3:::landsat-pds\"},\"object\":{\"key\":\"c1/L8/115/062/LC08_L1TP_115062_20171107_20171107_01_RT/index.html\",\"size\":5391,\"eTag\":\"0f06667fca1f707894bf579bd667e221\",\"sequencer\":\"005A023C441A8F8403\"}}}]}",
            "Timestamp" : "2017-11-07T23:05:40.219Z",
            "SignatureVersion" : "1",
            "Signature" : "bqrW1x6CgJntCz6f0F5uncyPZR+6ZM/tZ3OrRZDiudBv5DAtMyYR9n6KQ0aT+iYP5INfpL2GuIm8Uqco8ZHzg5AqEhHtNkpzGBQpQHvlF3t0ut9K27YNwJ6ZmnS14BgsLWyXIthVRjvHf1Hhx3ZInPMJrzTcKCOhOmBcM9zOpfWrHfnynuifpN3FaldDz6VY2d9QM0Rn8Fo8XZ4F+j01eAJVlydnRbSBbLewleuvhPQh6EG5r2EeekeniOIETrodS7o43ZClFr8OSgRE7BvpecVnnUEXBUIDDtRAPnIxo3Io0AmfPRI8xRfeKNhBIhPq3W3clm7Dxkp3N96OKoVUBw==",
            "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-433026a4050d206028891664da859041.pem",
            "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:274514004127:NewSceneHTML:7997d757-d1c6-4064-8935-34111968c8cc"
        }
        message_json = json.loads(sqs_message['Message'])
        image_key = message_json['Records'][0]['s3']['object']['key']
        d = datetime.datetime.strptime(sqs_message['Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

        rows = metadata_service.search(SpacecraftID.LANDSAT_8,
                                       start_date=d,
                                       sql_filters=["wrs_row=62", "wrs_path=115"])
        rows = metadata_service.search_aws('/imagery', wrs_path=115, wrs_row=62, collection_date=d)
        self.assertGreater(len(rows), 0)

        metadata = Metadata(rows[0])
        self.assertIsNotNone(metadata)

        landsat = Landsat(metadata)
        nda = landsat.fetch_imagery_array([4, 3, 2], [[0, 40000], [0, 40000], [0, 40000]], xRes=240, yRes=240)
        self.assertIsNotNone(nda)



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
        sql_filters = ['scene_id="LC80400312016103LGN00"']
        rows = metadata_service.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end,
                                       bounding_box=bounding_box,
                                       limit=1, sql_filters=sql_filters)
        self.m_row_data = rows[0]
        wkt_iowa = "POLYGON((-93.76075744628906 42.32707774458643,-93.47854614257812 42.32707774458643," \
                   "-93.47854614257812 42.12674735753131,-93.76075744628906 42.12674735753131," \
                   "-93.76075744628906 42.32707774458643))"
        self.iowa_polygon = loads(wkt_iowa)
        gdal.SetConfigOption('GDAL_VRT_ENABLE_PYTHON', "YES")

        d_start = date(2017, 3, 12)  # 2017-03-12
        d_end = date(2017, 3, 19)  # 2017-03-20, epl api is inclusive

        sql_filters = ['collection_number="PRE"']
        rows = self.metadata_service.search(
            SpacecraftID.LANDSAT_8,
            start_date=d_start,
            end_date=d_end,
            bounding_box=self.taos_shape.bounds,
            limit=10,
            sql_filters=sql_filters)

        for row in rows:
            self.metadata_set.append(Metadata(row, self.base_mount_path))

    def test_pixel_1(self):
        metadata = Metadata(self.m_row_data, self.base_mount_path)
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