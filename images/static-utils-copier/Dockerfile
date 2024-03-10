ARG UBUNTU_UTILS_BUILDER=registry.deckhouse.io/base_images/ubuntu:jammy-20221130@sha256:c14c3b1242536729ce5227ff833144977b4e378723858fb73a4cf40ea6daaf6a
ARG BASE_IMAGE=registry.deckhouse.io/base_images/alpine:3.16.3@sha256:5548e9172c24a1b0ca9afdd2bf534e265c94b12b36b3e0c0302f5853eaf00abb

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
FROM $UBUNTU_UTILS_BUILDER as util-linux-builder
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
FROM --platform=linux/amd64 $BASE_IMAGE

WORKDIR /

ADD bin-copier.sh  .

COPY --from=lvm-builder /lvm2/tools/lvm.static /usr/local/bin/flant/lvm.static
COPY --from=util-linux-builder /util-linux/lsblk /usr/local/bin/flant/lsblk.static
COPY --from=util-linux-builder /util-linux/lsblk /usr/local/bin/flant/nsenter.static

ENTRYPOINT ["/bin-copier.sh"]
CMD ["/usr/local/bin/flant", "/opt/deckhouse/bin"]
