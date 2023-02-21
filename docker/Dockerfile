FROM golang:1.20.1-bullseye

ARG PROTOBUF_VERSION=22.0

RUN apt update
RUN apt install unzip

WORKDIR /tmp/protoc
RUN curl -L https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip -o protoc.zip && \
  unzip protoc.zip && \
  mv bin/* /usr/local/bin/ && \
  mv include/* /usr/local/include/

WORKDIR /workspace
COPY go.mod .
COPY go.sum .
RUN go mod download

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28 && \
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
