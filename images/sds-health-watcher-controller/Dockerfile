ARG BASE_ALPINE=registry.deckhouse.io/base_images/alpine:3.16.3@sha256:5548e9172c24a1b0ca9afdd2bf534e265c94b12b36b3e0c0302f5853eaf00abb
ARG BASE_GOLANG_ALPINE_BUILDER=registry.deckhouse.io/base_images/golang:1.22.3-alpine@sha256:dbf216b880b802c22e3f4f2ef0a78396b4a9a6983cb9b767c5efc351ebf946b0

FROM $BASE_GOLANG_ALPINE_BUILDER as builder

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
