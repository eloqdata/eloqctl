#!/bin/bash
# Example:
#   ./build.bash build centos7 centos8 rocky9 ubuntu
#   ./build.bash test centos7 centos8 rocky9 ubuntu
set -e

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH= $(uname -m) ;;
esac

# BUILDX_PLATFORM='linux/amd64,linux/arm64'
build_image() {
    ln -s ${IMG_KIND}-${IMG_OS}.dockerfile Dockerfile
    BUILD_ARGS=""
    if [ $IMG_OS = "ubuntu" ]; then
        IMG_NAME="eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}${UBUNTU_ID}"
        BUILD_ARGS="--build-arg UBT_ID=${UBUNTU_ID}.04"
    else
        IMG_NAME="eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}"
    fi
    if [ -n "$BUILDX_PLATFORM" ]; then
        docker buildx build -t $IMG_NAME $BUILD_ARGS --platform $BUILDX_PLATFORM --push .
    else
        docker build -t ${IMG_NAME}-${ARCH} $BUILD_ARGS --platform linux/$ARCH .
        docker push $IMG_NAME
    fi
    rm Dockerfile
}

if [ -n "$1" ]; then
    IMG_KIND=$1
    for ((i = 2; i <= "$#"; i++)); do
        IMG_OS=${!i}
        if [ $IMG_OS = "ubuntu" ]; then
            for UBUNTU_ID in 18 20 22 24; do
                build_image
            done
        else
            build_image
        fi
    done
else
    IMG_KIND="build"
    IMG_OS="centos7"
    build_image
    IMG_OS="centos8"
    build_image
    IMG_OS="rocky9"
    build_image
    IMG_OS="ubuntu"
    for UBUNTU_ID in 18 20 22 24; do
        build_image
    done

    IMG_KIND="test"
    IMG_OS="centos7"
    build_image
    IMG_OS="centos8"
    build_image
    IMG_OS="rocky9"
    build_image
    IMG_OS="ubuntu"
    for UBUNTU_ID in 18 20 22 24; do
        build_image
    done
fi

echo "Done!"
