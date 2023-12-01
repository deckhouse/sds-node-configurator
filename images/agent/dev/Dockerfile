FROM golang:1.20-alpine3.18 as builder

RUN mkdir /go/src/storagecontroller
WORKDIR /go/src/storagecontroller

ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .

WORKDIR /go/src/storagecontroller/cmd/bc
RUN GOOS=linux GOARCH=amd64 go  build -o stctrl

FROM amd64/alpine:3.16.3

RUN apk update && apk add lvm2 lsblk curl

WORKDIR /root/
COPY --from=builder /go/src/storagecontroller/cmd/bc .

ENV NODE_NAME=test-node

CMD ["./stctrl"]
