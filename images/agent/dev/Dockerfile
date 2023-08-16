FROM golang:1.20-alpine3.18 as builder

RUN mkdir /go/src/storagecontroller
WORKDIR /go/src/storagecontroller

COPY . .
RUN go mod download
WORKDIR /go/src/storagecontroller/cmd/bc
RUN GOOS=linux GOARCH=amd64 go  build -o stctrl

FROM amd64/ubuntu:jammy-20230624

RUN apt-get update && apt-get install -y curl

WORKDIR /root/
COPY --from=builder /go/src/storagecontroller/cmd/bc .

ENV NODE_NAME=test-node

CMD ["./stctrl"]
