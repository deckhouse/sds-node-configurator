FROM golang:1.20-alpine3.18 as builder

RUN mkdir /go/src/storagecontroller
WORKDIR /go/src/storagecontroller

COPY . .
RUN go mod download
WORKDIR /go/src/storagecontroller/cmd/bc
RUN CGO_ENABLE=0 GOOS=linux go build -a -installsuffix cgo -o stctrl

FROM ubuntu:latest
WORKDIR /root/
COPY --from=builder /go/src/storagecontroller/cmd/bc .

ENV NODE_NAME=test-node

CMD ["./stctrl"]