#!/bin/bash
if [ $ERR_EXIT = true ]; then
    set -e
fi

list_jobs() {
    fly -t ${TARGET} jobs -p ${PIPELINE}
}

list_jobs

list_jobs | awk '{print $1}' |
    while read JOB; do
        echo "trigger job => ${PIPELINE}/${JOB}"
        time fly -t ${TARGET} trigger-job --job ${PIPELINE}/${JOB} --watch >${PIPELINE}.${JOB} 2>&1
        case $? in
        0) echo "Success" ;;
        3) echo "Aborted" ;;
        *) echo "Failed ($?)" ;;
        esac
    done

list_jobs
