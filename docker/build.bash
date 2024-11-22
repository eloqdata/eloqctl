#!/bin/bash
set -e

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH= $(uname -m) ;;
esac

# According to https://github.com/multiarch/alpine/issues/32, execute this command if you build failed with permission error:
# docker run --privileged multiarch/qemu-user-static:latest --reset -p yes --credential yes
BUILDX_PLATFORM='linux/amd64'
# BUILDX_PLATFORM='linux/amd64,linux/arm64'
build_image() {
    IMG_KIND=$1
    IMG_OS=$2
    OS_VER=$3
    if [ $IMG_OS = "ubuntu" ]; then
        BUILD_ARGS="--build-arg UBT_ID=${OS_VER}.04"
        ln -s ${IMG_KIND}-ubuntu.dockerfile Dockerfile
    else
        ln -s ${IMG_KIND}-${IMG_OS}${OS_VER}.dockerfile Dockerfile
    fi
    IMG_NAME="eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}${OS_VER}"
    if [ -n "$BUILDX_PLATFORM" ]; then
        docker buildx build -t $IMG_NAME $BUILD_ARGS --platform $BUILDX_PLATFORM --push .
    else
        docker build -t ${IMG_NAME}-${ARCH} $BUILD_ARGS --platform linux/$ARCH .
        docker push $IMG_NAME
    fi
    rm Dockerfile
}

if [ -n "$1" ]; then
    build_image $1 $2 $3
else
    build_image build centos 7
    build_image build centos 8
    build_image build rocky 9
    build_image build ubuntu 18
    build_image build ubuntu 20
    build_image build ubuntu 22
    build_image build ubuntu 24

    build_image test centos 7
    build_image test centos 8
    build_image test rocky 9
    build_image test ubuntu 18
    build_image test ubuntu 20
    build_image test ubuntu 22
    build_image test ubuntu 24
fi

echo "Done!"
