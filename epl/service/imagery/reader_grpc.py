import grpc
import time

import numpy as np

from typing import List

import epl.imagery.reader as imagery_reader
import epl.service.imagery.epl_imagery_api_pb2 as epl_imagery_api_pb2
import epl.service.imagery.epl_imagery_api_pb2_grpc as epl_imagery_api_pb2_grpc

from shapely.geometry import shape
from datetime import datetime
from concurrent import futures

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class ImageryServicer():
    def __init__(self):
        print("start")

    @staticmethod
    def _clean_band_definitions(request_band_definitions: List[epl_imagery_api_pb2.BandDefinition]):
        band_definitions = []
        for request_band in request_band_definitions:
            if request_band.HasField("band_function"):
                request_function_details = request_band.band_function
                inner_band_definitions = ImageryServicer._clean_band_definitions(request_band.band_definitions)
                function_details = imagery_reader.FunctionDetails(name=request_function_details.name,
                                                                  band_definitions=inner_band_definitions,
                                                                  data_type=request_function_details.data_type,
                                                                  code=request_function_details.code,
                                                                  arguments=request_function_details.arguements,
                                                                  transfer_type=request_function_details.transfer_type)
                band_definitions.append(function_details)
            elif request_band.band_type != 0:
                band_definitions.append(imagery_reader.Band[epl_imagery_api_pb2.Band.Name(request_band.band_type)])
            else:
                band_definitions.append(request_band.band_number)

        return band_definitions

    def ImagerySearchNArray(self,
                            request: epl_imagery_api_pb2.ImageryRequest,
                            context) -> epl_imagery_api_pb2.NDArrayResult:
        metadata_list = [imagery_reader.Metadata(metadata) for metadata in request.metadata]
        landsat = imagery_reader.Landsat(metadata_list)
        band_definitions = ImageryServicer._clean_band_definitions(request.band_definitions)

        # only select scaling params for the bands you want
        scale_params = list(map(lambda x: x.scale_params, (filter(lambda x: len(x.scale_params) > 0, request.band_definitions))))

        output_data_type = imagery_reader.DataType[epl_imagery_api_pb2.GDALDataType.Name(request.output_type)]
        nd_array = landsat.fetch_imagery_array(band_definitions=band_definitions,
                                               envelope_boundary=tuple(request.extent),
                                               polygon_boundary_wkb=request.cutline_wkb,
                                               scale_params=scale_params,
                                               boundary_cs=request.extent_cs,
                                               output_type=imagery_reader.DataType[
                                                   epl_imagery_api_pb2.GDALDataType.Name(request.output_type)],
                                               spatial_resolution_m=request.spatial_resolution_m)

        result = epl_imagery_api_pb2.NDArrayResult()
        result.shape.extend(nd_array.shape)

        # Complex not supported yet...
        result.dtype = request.output_type
        if output_data_type.grpc_num == epl_imagery_api_pb2.BYTE or \
            output_data_type.grpc_num == epl_imagery_api_pb2.UINT16 or \
            output_data_type.grpc_num == epl_imagery_api_pb2.UINT32:
            result.data_uint32.extend(nd_array.ravel())
        elif output_data_type.grpc_num == epl_imagery_api_pb2.INT16 or \
            output_data_type.grpc_num == epl_imagery_api_pb2.INT32:
            result.data_int32.extend(nd_array.ravel())
        elif output_data_type.grpc_num == epl_imagery_api_pb2.UINT32:
            result.data_uint32.extend(nd_array.ravel())
        elif output_data_type.grpc_num == epl_imagery_api_pb2.FLOAT32:
            result.data_float.extend(nd_array.ravel())
        elif output_data_type.grpc_num == epl_imagery_api_pb2.FLOAT64:
            result.data_double.extend(nd_array.ravel())

        return result

    def MetadataSearch(self,
                       request: epl_imagery_api_pb2.MetadataRequest,
                       context) -> epl_imagery_api_pb2.MetadataResult:
        result = epl_imagery_api_pb2.MetadataResult()
        metadata_service = imagery_reader.MetadataService()

        start_date = None if not request.HasField("start_date") else datetime.strptime(
            request.start_date.ToJsonString(), _DATE_FORMAT)
        end_date = None if not request.HasField("end_date") else datetime.strptime(request.end_date.ToJsonString(),
                                                                                   _DATE_FORMAT)

        metadata_generator = metadata_service.search(satellite_id=imagery_reader.SpacecraftID(request.satellite_id),
                                                     bounding_box=request.bounding_box,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     sort_by=request.sort_by,
                                                     limit=10 if request.limit == 0 else request.limit,
                                                     sql_filters=request.sql_filters)

        for metadata in metadata_generator:
            for attr, value in vars(metadata).items():
                # TODO rename bounding_box
                if attr == "bounds":
                    result.bounds.extend(value)
                elif not attr.startswith("_Metadata__"):
                    setattr(result, attr, value)

            result.wrs_polygon_wkb.extend([shape(metadata.get_wrs_polygon()).wkb])

            # epl_imagery_api_pb2.ServiceGeometry(geometry_enoding_type=epl_imagery_api_pb2.GeometryEncodingType.geojson,
            #                                     geometry_string=metadata.get_wrs_polygon())

            yield result


def serve():
    # options=(('grpc.max_message_length', <a large integer of your choice>,),))
    MB = 1024 * 1024
    # https://github.com/grpc/grpc/issues/7927
    GRPC_CHANNEL_OPTIONS = [('grpc.max_message_length', 64 * MB), ('grpc.max_receive_message_length', 64 * MB)]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=GRPC_CHANNEL_OPTIONS)
    epl_imagery_api_pb2_grpc.add_ImageryOperatorsServicer_to_server(ImageryServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
