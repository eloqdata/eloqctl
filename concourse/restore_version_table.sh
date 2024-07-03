#!/bin/bash
set -eo

OUTFILE='/tmp/restore_version_table.sql'
VER_PATTERN='(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)'
OS_LIST='rhel7 rhel8 ubuntu18 ubuntu20 ubuntu22 ubuntu24'

walk_s3_dir() {
  for OS_ID in ${OS_LIST}; do
    aws s3 ls s3://eloq-release/${PRODUCT}/${OS_ID}/${KVS_ID}/${PRODUCT} | awk '{print $NF}' |
      while read FILENAME; do
        local VERSION=$(echo ${FILENAME%.tar.gz} | awk -F'-' '{print $2}')
        if [[ ! "${VERSION}" =~ ${VER_PATTERN} ]]; then
          echo "Ignore version ${VERSION}"
          continue
        fi
        local ARCH=$(echo ${FILENAME%.tar.gz} | awk -F'-' '{print $3}')
        local INSERT_SQL="INSERT INTO tx_release VALUES ('${PRODUCT}', '${ARCH}', '${OS_ID}', '${KVS_ID}', $(echo ${VERSION} | tr '.' ','));"
        echo ${INSERT_SQL} >>${OUTFILE}
      done
  done
}

PRODUCT='eloqsql'
for KVS_ID in cassandra dynamodb; do
  walk_s3_dir
done

PRODUCT='eloqkv'
for KVS_ID in cassandra rocksdb rocks_s3 rocks_gcs; do
  walk_s3_dir
done

echo "sql file dumped: ${OUTFILE}"

#psql "postgresql://postgres:eloq-pub-service-postgresql@18.177.72.104:5432/eloq_release?sslmode=require" -f ${OUTFILE}
#echo "restore version table done !!"
