#!/bin/bash

version=$(git log --date=iso --pretty=format:"%cd @%H" -1)
if [ $? -ne 0 ]; then
    version="unknown version"
fi

compile="$(date +'%F %T %z') by $(rustc --version)"
if [ $? -ne 0 ]; then
    compile="unknown datetime"
fi

describe=$(git describe --tags 2>/dev/null)
if [ $? -eq 0 ]; then
    version="${version} @${describe}"
fi

cat << EOF > $1/version
version = $version
compile = $compile
EOF
