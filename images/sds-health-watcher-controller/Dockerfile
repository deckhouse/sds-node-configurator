ARG BASE_ALPINE=registry.deckhouse.io/base_images/alpine:3.16.3@sha256:5548e9172c24a1b0ca9afdd2bf534e265c94b12b36b3e0c0302f5853eaf00abb
ARG BASE_GOLANG_20_ALPINE_BUILDER=registry.deckhouse.io/base_images/golang:1.22.1-alpine@sha256:0de6cf7cceab6ecbf0718bdfb675b08b78113c3709c5e4b99456cdb2ae8c2495

FROM $BASE_GOLANG_20_ALPINE_BUILDER as builder

WORKDIR /go/src
ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .
WORKDIR /go/src/cmd
RUN GOOS=linux GOARCH=amd64 go build -o sds-health-watcher-controller

FROM --platform=linux/amd64 $BASE_ALPINE
COPY --from=builder /go/src/cmd/sds-health-watcher-controller /go/src/cmd/sds-health-watcher-controller

ENTRYPOINT ["/go/src/cmd/sds-health-watcher-controller"]
