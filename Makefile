#rebuild the protobuf type definitions
protoc:
	protoc -I ./grpc/proto/ ./grpc/proto/plugin.proto --go_out=./grpc/proto/ --go-grpc_out=./grpc/proto/

scaffold:
	go build -o /usr/local/bin/tailpipe-scaffold