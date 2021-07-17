PROTO_SPECS = \
  ./proto/mipt_distencode/manager/manager.proto \
  ./proto/mipt_distencode/mgmt_messages.proto

proto: proto_manager
	

proto_manager:
	python -m grpc_tools.protoc \
	  -I./proto \
	  --python_out=. --grpc_python_out=. \
	  $(PROTO_SPECS)

ssl_child: ssl_ca
	./scripts/make-child.sh ${SSL_SUBJ}

ssl_ca:
	./scripts/make-ca.sh ${SSL_CA_SUBJ}

clean: rm_pycache
	

rm_pycache:
	find mipt_distencode -type d -name __pycache__ -exec rm -r {} \;
