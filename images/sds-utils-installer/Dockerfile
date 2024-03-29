ARG UBUNTU_UTILS_BUILDER=registry.deckhouse.io/base_images/ubuntu:jammy-20221130@sha256:c14c3b1242536729ce5227ff833144977b4e378723858fb73a4cf40ea6daaf6a
ARG BASE_GOLANG_22_ALPINE=registry.deckhouse.io/base_images/golang:1.22.1-alpine@sha256:0de6cf7cceab6ecbf0718bdfb675b08b78113c3709c5e4b99456cdb2ae8c2495
ARG BASE_IMAGE=registry.deckhouse.io/base_images/alpine:3.16.3@sha256:5548e9172c24a1b0ca9afdd2bf534e265c94b12b36b3e0c0302f5853eaf00abb
ARG SOURCE_REPO

#################################
FROM $UBUNTU_UTILS_BUILDER as lvm-builder
ARG SOURCE_REPO
# 2.03.23(2). Include fix for this issue: https://github.com/lvmteam/lvm2/issues/128
ARG LVM_VERSION=d786a8f820d54ce87a919e6af5426c333c173b11

RUN apt-get update && apt-get install -y \
    build-essential \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libdevmapper-dev \
    libaio-dev \
    libblkid-dev \
    thin-provisioning-tools \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /lvm2

RUN git clone ${SOURCE_REPO}/lvmteam/lvm2.git . && \
  git checkout ${LVM_VERSION} && \
  ./configure --enable-static_link --disable-silent-rules \
    --disable-readline --enable-blkid_wiping --build=x86_64-linux-gnu && \
  make

#################################
FROM $BASE_GOLANG_22_ALPINE as bin-copier-builder
WORKDIR /go/src

ADD go.mod .
ADD go.sum .

RUN go mod download

COPY bin-copier.go .

RUN GOOS=linux GOARCH=amd64 go build -o /bin-copier

################################
FROM --platform=linux/amd64 $BASE_IMAGE

WORKDIR /
COPY --from=lvm-builder /lvm2/tools/lvm.static /sds-utils/bin/lvm.static
COPY --from=bin-copier-builder /bin-copier /bin-copier


ENTRYPOINT ["/bin-copier"]
CMD ["/sds-utils", "/opt/deckhouse/sds"]
