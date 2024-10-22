#!/bin/sh

initialize_env() {
    ## Create virtual env for python with trino and boto3
    if [[ ! -d ./trino_venv ]]; then
        echo "INFO: Create virtual environment $(pwd)/trino_venv"
        python3 -m venv trino_venv
        . ./trino_venv/bin/activate
        pip3 install -q --upgrade pip
        pip3 install -q trino boto3
        deactivate
    fi

    ## Genererate config.ini from template file
    for template in $(find . -name '*.template'); do
         rm -f ${template/.template/}
         awk '
           (FILENAME!="'${template}'" && match($0,/=/)) {
               a[substr($0,1,RSTART-1)]=substr($0,RSTART+1);
               next
           }
           (FILENAME=="'${template}'") {
               while(match($0,/\$\{ENV:([^}]*)\}/)) {
                   x=substr($0,RSTART+6,RLENGTH-7);
                   if (x in a) {
                       $0=substr($0,1,RSTART-1) a[x] substr($0,RSTART+RLENGTH)
                   } else {
                       break
                   }
               }
           } 1
         ' ./private/*.env "${template}" > ./private/${template/.template/}
    done
}

initialize_minio() {
    ## Create user and bucket if this is a local minio S3
    export $(cat ./private/minio.env)
    if [[ ${S3_ENDPOINT} =~ 'minio:' ]]; then
        mc alias set unittest https://${S3_ENDPOINT} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
        mc admin user add unittest "${S3_KEY}" "${S3_SECRET}"
        mc admin policy attach unittest readwrite --user "${S3_KEY}"
        mc mb --ignore-existing "unittest/${S3_BUCKET}"
   fi
}

runtest() {
    ## Check if config.ini needs to be edited 
    if grep -q '\${ENV:' ./private/config.ini; then
        echo "WARN -- Edit private/config.ini"
    fi

    ## Activate the environment
    if [[ "${VIRTUAL_ENV-x}" != "$(pwd)/trino_venv" ]]; then
        echo "INFO -- Activate the virtual env:  . $(pwd)/trino_venv/bin/activate"
    fi

    . ./trino_venv/bin/activate

    echo "INFO -- Run the tests: PYTHONWARNINGS='ignore:Unverified HTTPS request' python3 -m unittest"
    python3 -m unittest ${@}
}

main() {
    set -euE
    if [[ ! -d /unittest ]]; then
        ## Unfortunately a *nix shell is required to bootstrap this script. If necessary, you must
        #  replace the docker commands of this if block with the equivalent of your favorite shell
        cd $(dirname ${BASH_SOURCE-$0})
        if [[ "${1-x}" == '-f' ]]; then
            shift
            echo "clear environment"
            rm -rf ./private
        fi
        mkdir -m 0700 -p ./private
        rsync -a ../containers/minio.env ./private/minio.env
        docker run --rm --quiet --volume ${PWD}/private/minio.env:/unittest/private/minio.env:ro \
                                --volume ${PWD}/../containers/.workdir/certs/public.crt:/tmp/.mc/certs/CAs/ca-bundle.crt:ro \
                                --volume ${PWD}/:/unittest \
                                --network trinohack_trinocluster \
                                --entrypoint '' quay.io/minio/minio:latest /bin/sh -c "/unittest/test.sh -mc"
        docker run --rm --quiet --volume ${PWD}/private/minio.env:/unittest/private/minio.env:ro \
                                --volume ${PWD}/../containers/.workdir/certs/public.crt:/tmp/.mc/certs/CAs/ca-bundle.crt:ro \
                                --volume ${PWD}/:/unittest \
                                --network trinohack_trinocluster \
                                --entrypoint '' python:3.12-alpine /bin/sh -c "/unittest/test.sh -test $*"
    elif [[ "${1}" == '-mc' ]]; then
        shift
        cd /unittest
        initialize_minio
    elif [[ "${1}" == '-test' ]]; then
        shift
        cd /unittest
        initialize_env
        runtest ${@}
    fi
}

main "${@}"
