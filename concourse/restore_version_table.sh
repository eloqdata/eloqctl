#!/bin/bash
set -eo

OUTFILE="/tmp/restore_version_table.sql"
VER_PATTERN="(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)"
restore_table() {
  for OS in centos7 centos8 ubuntu1804 ubuntu2004; do 
    aws s3 ls s3://eloq-release/${PRODUCT}/${OS}/${STORE}/${PRODUCT} | awk '{print $NF}' |
    while read FILENAME; do
      local VERSION=$(echo ${FILENAME%.tar.gz} | awk -F'-' '{print $2}')
      if [[ ! "${VERSION}" =~ ${VER_PATTERN} ]]; then
        echo "Ignore version ${VERSION}"
        continue
      fi
      local ARCH=$(echo ${FILENAME%.tar.gz} | awk -F'-' '{print $3}')
      local INSERT_SQL="INSERT INTO tx_release VALUES ('${PRODUCT}', '${ARCH}', '${OS}', '${STORE}', $(echo ${VERSION} | tr '.' ','));"
      echo ${INSERT_SQL} >> ${OUTFILE}
    done
  done
}

PRODUCT="eloqsql"
for STORE in cassandra dynamodb; do
  restore_table
done

PRODUCT="eloqkv"
for STORE in cassandra rocksdb rocksdb_s3 rocksdb_gcs; do              
  restore_table
done

echo "sql file dumped: ${OUTFILE}"

psql "postgresql://postgres:eloq-pub-service-postgresql@18.177.72.104:5432/eloq_release?sslmode=require" -f ${OUTFILE}

echo "restore version table done !!"