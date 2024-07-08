#!/bin/bash
set -e

# PLATFORM='linux/amd64,linux/arm64'
build_image() {
    ln -s ${IMG_KIND}-${IMG_OS}.dockerfile Dockerfile
    if [ -z "$PLATFORM" ]; then
        docker build -t monographdb/waiter-${IMG_KIND}-${IMG_OS} .
        docker push monographdb/waiter-${IMG_KIND}-${IMG_OS}
    else
        docker buildx build --platform ${PLATFORM} -t monographdb/waiter-${IMG_KIND}-${IMG_OS} --push .
    fi
    rm Dockerfile
}

IMG_KIND=$1
for ((i = 2; i <= "$#"; i++)); do
    IMG_OS=${!i}
    build_image
done

echo "Done!"
