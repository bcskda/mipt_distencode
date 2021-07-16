PROTO_SPECS = \
  ./proto/mipt_distencode/manager/manager.proto \
  ./proto/mipt_distencode/mgmt_messages.proto

proto: proto_manager
	

proto_manager:
	python -m grpc_tools.protoc \
	  -I./proto \
	  --python_out=. --grpc_python_out=. \
	  $(PROTO_SPECS)
