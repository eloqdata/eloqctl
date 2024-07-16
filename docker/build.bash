#!/bin/bash
set -e

# PLATFORM='linux/amd64,linux/arm64'
build_image() {
    ln -s ${IMG_KIND}-${IMG_OS}.dockerfile Dockerfile
    BUILD_ARGS=""
    if [ $IMG_OS = "ubuntu" ]; then
        IMG_NAME="monographdb/waiter-${IMG_KIND}-${IMG_OS}${UBUNTU_ID}"
        BUILD_ARGS="--build-arg OS_ID=${UBUNTU_ID}"
    else
        IMG_NAME="monographdb/waiter-${IMG_KIND}-${IMG_OS}"
    fi
    if [ -n "$PLATFORM" ]; then
        docker buildx build --platform $PLATFORM -t $IMG_NAME $BUILD_ARGS --push .
    else
        docker build -t $IMG_NAME $BUILD_ARGS .
        docker push $IMG_NAME
    fi
    rm Dockerfile
}

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

echo "Done!"
