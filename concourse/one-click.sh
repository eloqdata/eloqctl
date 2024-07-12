#!/bin/bash

list_jobs() {
    fly -t ${TARGET} jobs -p ${PIPELINE}
}

list_jobs

list_jobs | awk '{print $1}' |
    while read JOB; do
        echo "trigger job => ${PIPELINE}/${JOB}"
        time fly -t ${TARGET} trigger-job --job ${PIPELINE}/${JOB} --watch >${PIPELINE}.${JOB} 2>&1
        echo "finished with $?"
    done

list_jobs
