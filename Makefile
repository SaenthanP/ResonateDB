PROTO_DIR := protos/cluster
OUT_DIR := proto-gen

PROTO_FILES := $(PROTO_DIR)/cluster.proto

all: generate

# Generate Go protobuf and gRPC code
generate:
	mkdir -p $(OUT_DIR)/cluster
	protoc \
		--go_out=$(OUT_DIR) \
		--go-grpc_out=$(OUT_DIR) \
		$(PROTO_FILES)

# Clean generated code
clean:
	rm -rf $(OUT_DIR)/cluster/*.pb.go
