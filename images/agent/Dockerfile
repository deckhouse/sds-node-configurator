ARG GOLANG_20_ALPINE_BUILDER=registry.deckhouse.io/base_images/golang:1.20.5-alpine3.18@sha256:51a47fb0851397db2f506c15c426735bc23de31177cbdd962880c0879d1906a4
ARG UBUNTU_UTILS_BUILDER=registry.deckhouse.io/base_images/ubuntu:jammy-20221130@sha256:c14c3b1242536729ce5227ff833144977b4e378723858fb73a4cf40ea6daaf6a
ARG BASE_IMAGE=registry.deckhouse.io/base_images/scratch@sha256:b054705fcc9f2205777d80a558d920c0b4209efdc3163c22b5bfcb5dda1db5fc

#################################
FROM $UBUNTU_UTILS_BUILDER as lvm-builder
RUN apt-get update && apt-get install -y \
    build-essential \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libdevmapper-dev \
    libaio-dev \
    thin-provisioning-tools \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://sourceware.org/git/lvm2.git /lvm2

WORKDIR /lvm2
RUN git checkout v2_03_22
RUN ./configure --enable-static_link
RUN make

#################################
FROM $UBUNTU_UTILS_BUILDER as lsblk-builder
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    autopoint \
    autoconf \
    bison \
    libtool \
    automake \
    gettext \
    flex \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/util-linux/util-linux.git /util-linux

WORKDIR /util-linux
RUN git checkout v2.39.3
RUN ./autogen.sh
RUN ./configure LDFLAGS="-static" --enable-static-programs -disable-shared
RUN make LDFLAGS="--static" lsblk nsenter


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

COPY --from=lvm-builder /lvm2/tools/lvm.static /usr/local/bin/lvm.static
COPY --from=lsblk-builder /util-linux/lsblk /usr/local/bin/lsblk.static
COPY --from=lsblk-builder /util-linux/nsenter /usr/local/bin/nsenter.static
COPY --from=agent-builder /go/src/cmd/sds-node-configurator-agent /go/src/cmd/sds-node-configurator-agent

CMD ["/go/src/cmd/sds-node-configurator-agent"]
