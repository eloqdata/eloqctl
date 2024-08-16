#!/bin/bash
set -e

new_manifest() {
    IMG_KIND=$1
    IMG_OS=$2
    docker manifest create --amend eloqdata/eloqctl-${IMG_KIND}-${IMG_OS} eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}-amd64 eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}-arm64
    docker manifest push eloqdata/eloqctl-${IMG_KIND}-${IMG_OS}
}

if [ -n "$1" ]; then
    new_manifest $1 $2
else
    docker manifest create --amend eloqdata/eloqctl-build-centos7 eloqdata/eloqctl-build-centos7-amd64
    docker manifest push eloqdata/eloqctl-build-centos7

    new_manifest build centos8
    new_manifest build rocky9
    new_manifest build ubuntu18
    new_manifest build ubuntu20
    new_manifest build ubuntu22
    new_manifest build ubuntu24

    new_manifest test centos7
    new_manifest test centos8
    new_manifest test rocky9
    new_manifest test ubuntu18
    new_manifest test ubuntu20
    new_manifest test ubuntu22
    new_manifest test ubuntu24
fi

echo "Done!"
