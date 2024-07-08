#!/bin/bash
set -exuo
IMG_KIND=$1
IMG_OS=$2

rm Dockerfile
ln -s ${IMG_KIND}-${IMG_OS}.dockerfile Dockerfile
docker build -t monographdb/waiter-${IMG_KIND}-${IMG_OS} .
docker push monographdb/waiter-${IMG_KIND}-${IMG_OS}
