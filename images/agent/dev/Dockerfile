FROM golang:1.20-alpine3.18 as builder

RUN mkdir /go/src/storagecontroller
WORKDIR /go/src/storagecontroller

ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .

WORKDIR /go/src/storagecontroller/cmd/bc
RUN GOOS=linux GOARCH=amd64 go  build -o stctrl

FROM amd64/ubuntu:jammy-20231128
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y lvm2 curl && rm -rf /var/lib/apt/lists/*

WORKDIR /root/
COPY --from=builder /go/src/storagecontroller/cmd/bc .

ENV NODE_NAME=test-node

CMD ["./stctrl"]
