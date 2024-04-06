ARG GOLANG_20_ALPINE_BUILDER=registry.deckhouse.io/base_images/golang:1.20.5-alpine3.18@sha256:51a47fb0851397db2f506c15c426735bc23de31177cbdd962880c0879d1906a4
ARG UBUNTU_UTILS_BUILDER=registry.deckhouse.io/base_images/ubuntu:jammy-20221130@sha256:c14c3b1242536729ce5227ff833144977b4e378723858fb73a4cf40ea6daaf6a
ARG BASE_IMAGE=registry.deckhouse.io/base_images/scratch@sha256:b054705fcc9f2205777d80a558d920c0b4209efdc3163c22b5bfcb5dda1db5fc

#################################
FROM $UBUNTU_UTILS_BUILDER as util-linux-builder
ARG SOURCE_REPO
ARG UTIL_LINUX_VERSION=2.39.3

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    autopoint \
    autoconf \
    bison \
    libtool \
    libudev-dev \
    libblkid-dev \
    libsmartcols-dev \
    libmount-dev \
    automake \
    gettext \
    flex \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /util-linux

RUN git clone ${SOURCE_REPO}/util-linux/util-linux.git . && \
  git checkout v${UTIL_LINUX_VERSION} && \
  ./autogen.sh

RUN ./configure LDFLAGS="-static" --enable-static-programs -disable-all-programs --enable-nsenter && \
    make install-strip

RUN ./configure --prefix /opt/deckhouse/sds --with-udev && \
    make install-strip


#################################
FROM $GOLANG_20_ALPINE_BUILDER as agent-builder
WORKDIR /go/src

ADD go.mod .
ADD go.sum .

RUN go mod download

COPY . .

WORKDIR /go/src/cmd
RUN GOOS=linux GOARCH=amd64 go build -o sds-node-configurator-agent

#################################
FROM --platform=linux/amd64 $BASE_IMAGE

COPY --from=util-linux-builder /util-linux/nsenter.static /opt/deckhouse/sds/bin/nsenter.static

# Got from ldd lsblk.dynamic
COPY --from=util-linux-builder /opt/deckhouse/sds/lib/libblkid.so.1 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /opt/deckhouse/sds/lib/libmount.so.1 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /opt/deckhouse/sds/lib/libsmartcols.so.1 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /lib/x86_64-linux-gnu/libudev.so.1 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /lib/x86_64-linux-gnu/libc.so.6 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /lib/x86_64-linux-gnu/libcap.so.2 /opt/deckhouse/sds/lib/
COPY --from=util-linux-builder /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY --from=util-linux-builder /opt/deckhouse/sds/bin/lsblk /opt/deckhouse/sds/bin/lsblk.dynamic

COPY --from=agent-builder /go/src/cmd/sds-node-configurator-agent /go/src/cmd/sds-node-configurator-agent

CMD ["/go/src/cmd/sds-node-configurator-agent"]
