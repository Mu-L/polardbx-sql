./protoc --java_out=./ *.proto
./protoc --plugin=protoc-gen-grpc-java=./protoc-gen-grpc-java --grpc-java_out=./ *.proto
