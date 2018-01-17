import grpc

import epl.service.imagery.epl_imagery_api_pb2 as epl_imagery_api_pb2
import epl.service.imagery.epl_imagery_api_pb2_grpc as epl_imagery_api_pb2_grpc


def run():
  channel = grpc.insecure_channel('localhost:50051')
  stub = epl_imagery_api_pb2_grpc.ImageryOperatorsStub(channel)
  request = epl_imagery_api_pb2.MetadataRequest()
  result = stub.MetadataSearch(request)
  print("test")

  # print("-------------- GetFeature --------------")
  # guide_get_feature(stub)
  # print("-------------- ListFeatures --------------")
  # guide_list_features(stub)
  # print("-------------- RecordRoute --------------")
  # guide_record_route(stub)
  # print("-------------- RouteChat --------------")
  # guide_route_chat(stub)


if __name__ == '__main__':
  run()