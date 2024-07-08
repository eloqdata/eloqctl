#!/bin/bash
set -exuo

build_image() {
    rm Dockerfile
    ln -s ${IMG_KIND}-${IMG_OS}.dockerfile Dockerfile
    docker build -t monographdb/waiter-${IMG_KIND}-${IMG_OS} .
    docker push monographdb/waiter-${IMG_KIND}-${IMG_OS}
}

IMG_KIND=$1
for ((i = 2; i <= "$#"; i++)); do
    IMG_OS=${!i}
    build_image
done
