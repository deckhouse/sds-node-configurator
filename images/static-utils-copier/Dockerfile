ARG UBUNTU_UTILS_BUILDER=registry.deckhouse.io/base_images/ubuntu:jammy-20221130@sha256:c14c3b1242536729ce5227ff833144977b4e378723858fb73a4cf40ea6daaf6a
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
    automake \
    gettext \
    flex \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /util-linux

RUN git clone ${SOURCE_REPO}/util-linux/util-linux.git . && \
  git checkout v${UTIL_LINUX_VERSION} && \
  ./autogen.sh && \
  ./configure LDFLAGS="-static" --enable-static-programs -disable-shared

RUN make LDFLAGS="--static" lsblk

#################################
FROM --platform=linux/amd64 $BASE_IMAGE

RUN apk add --no-cache rsync

WORKDIR /

COPY --from=lvm-builder /lvm2/tools/lvm.static /static-bin/lvm.static
COPY --from=util-linux-builder /util-linux/lsblk /static-bin/lsblk.static

ADD bin-copier.sh  .

ENTRYPOINT ["/bin-copier.sh"]
CMD ["/static-bin", "/opt/deckhouse/sds"]
