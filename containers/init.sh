#!/bin/bash

info() {
    printf "${Gr-} INFO ${Nc-} -- %s\n" "$*"
}

warn_() {
    printf "${Br-} WARN ${Nc-} -- %s\n" "$*"
}

warn() {
    warn_ "$@"
    issues+=1
}

errr() {
    printf "${Re-} ERR  ${Nc-}  -- %s\n" "$*"
    issues+=1
}

get_curl() {
    curl -sL -o "${1}" -C - "${2}"
}

get_wget() {
    #untested
    wget -cqL -O "${1}" "${2}"
}

get_downloader() {
    command -v curl >& /dev/null && echo get_curl && return
    command -v wget >& /dev/null && echo get_wget && return
    echo "errr Install curl or wget to download "
}

apply_templates() {
    info "Apply templates"
    for template in $(find "${workdir}"/config -name '*.template'); do
         info "${template}"
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
         ' ./*.env "${template}" > ${template/.template/}
    done
}

gen_certs() {
    if [[ -e "${workdir}/certs/public.crt" && -e "${workdir}/certs/private.key" ]]; then
        return
    fi
    info "Generate TLS private key and certificate"
    mkdir -p -m 0700 "${workdir}/certs/CAs"
    openssl req -noenc -new -x509 -nodes -days 720 -keyout "${workdir}/certs/private.key" -out "${workdir}/certs/public.crt" -config openssl.cnf -subj /C=CH/ST=VD/L=Lausanne/O=EPFL/OU=Hackathon2024/CN=minio
}

gen_password() {
    for i in {1..5}; do echo {A..Z} {a..z} {0..9} "[ ] - @ _ ^ ~ : , %"|tr -s ' ' '\n'|sort -R|head -5; done|tr -d '\n'
}

gen_key() {
    if command -v shasum >& /dev/null; then
        gen_password|shasum -a 256|cut -c 1-20
    elif command -v sha256sum >& /dev/null; then
        gen_password|sha256sum|cut -c 1-20
    else
        errr "Shasum not installed"
        return 1
    fi
}

gen_minio_env() {
    [[ -e minio.env ]] && return 0
    info "Generate minio.env"
    local -r s3_root_pass=$(gen_key)
    local -r s3_key=$(gen_key)
    local -r s3_secret=$(gen_key)
    local -r s3_endpoint="minio:9000"
    local -r s3_bucket="hackathon"
    if [[ -e  minio.env ]]; then
        return
    fi
    cat<<-EOT > minio.env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=${s3_root_pass}
S3_KEY=${s3_key}
S3_SECRET=${s3_secret}
S3_ENDPOINT=${s3_endpoint}
S3_BUCKET=${s3_bucket}
EOT
    if (( in_docker==0 )); then
        declare -ar volumes=($(docker volume ls -q|grep '_minio_s3'||true))
        if (( ${#volumes[*]} > 0 )); then
            warn "delete existing minio volumes: docker volume rm ${volumes[*]}"
        fi
    fi
}

gen_mysql_env() {
    [[ -e mysql.env ]] && return 0
    info "Generate mysql.env"
    local -r hive_pass=$(gen_password)
    local -r root_pass=$(gen_password)
    if [[ -e  mysql.env ]]; then
        return
    fi
    cat<<-EOT > mysql.env
MYSQL_USER=hiveuser
MYSQL_PASSWORD=${hive_pass}
MYSQL_ROOT_PASSWORD=${root_pass}
EOT
    if (( in_docker==0 )); then
        declare -ar volumes=($(docker volume ls -q|grep '_mysqlb'||true))
        if (( ${#volumes[*]} > 0 )); then
            warn "delete existing mysql volumes: docker volume rm ${volumes[*]}"
        fi
    fi
}

gen_trino_env() {
    [[ -e trino.env ]] && return 0
    info "Generate trino.env"
    local -r catalog_management=dynamic
    cat<<-EOT > trino.env
CATALOG_MANAGEMENT=${catalog_management-false}
EOT
}

export_certs() {
    [[ -e "${workdir}/certs/cacerts" ]] && return 0
    info "Generate java jks cacerts"
    if (( in_docker == 0 )); then
        docker run --quiet --rm --volume "${PWD}/${workdir}/certs:/initialize/certs" --name extract --user root --entrypoint '' \
            "trinodb/trino:${trino_version}" \
            /bin/bash -c "keytool -cacerts -alias mycert -importcert -file /initialize/certs/public.crt -storepass changeit -noprompt; cp \${JAVA_HOME}/lib/security/cacerts /initialize/certs/CAs/"
    elif [[ -n "${JAVA_HOME}" ]]; then
        cp $JAVA_HOME/lib/security/cacerts ${workdir}/certs/CAs/cacerts
        keytool -keystore ${workdir}/certs/CAs/cacerts -alias mycert -importcert -file ${workdir}/certs/public.crt -storepass changeit -noprompt
    fi
}

usage() {
cat<<-EOT
$0 [-hfcv][-n 0|1]
-h       this help
-f       force reinitialization
-c       preview docker compose config
-v       display docker compose variables

Then:
  docker compose -p trinohack up -d
  docker compose -p trinohack scale trino-worker=n
  docker exec -it trinohack_trino-coordinator-1 trino
  docker compose -p trinohack down
EOT
}

cleanup() {
    info "Clean up existing settings"
    rm -rf "${workdir}"
    rm -f .env
    rm -f trino.env
    if (( in_docker == 0 )); then
        declare -ar minio_volumes=($(docker volume ls -q|grep '_minio_s3'||true))
        declare -ar mysql_volumes=($(docker volume ls -q|grep '_mysql'||true))
        if (( ${#mysql_volumes[*]} > 0 )); then
            warn_ "mysql.env not regenerated while a mysqldb volume may still exist: ${mysql_volumes[*]}"
        else
            rm -f mysql.env
        fi
        if (( ${#minio_volumes[*]} > 0 )); then
            warn_ "minio.env not regenerated while a minio_s3 volume may still exist: ${minio_volumes[*]}"
        else
            rm -f minio.env
        fi
    else
        for f in mysql.env minio.env; do
            if [[ -e ${f} ]]; then
                warn_ "pre-existing ${f} not regenerated"
            fi
        done
    fi
}

error() {
    errr "Aborted."
}

set_env() {
    grep -q "${1}" .env >&/dev/null && return
    if [[ ! -e "${2}" ]]; then
        echo "${1}=${2}" >> .env 
    else
        grep "${1}" "${2}" >> .env 
    fi
}

main() {
    set -euE
    declare -r  mysql_version=9.1.0
    declare -r  mysql_connector_version=9.1.0
    declare -r  trino_version=460
    declare -r  mysql_repo=container-registry.oracle.com/mysql/community-server
    declare -ar mysql_image=(arm64=${mysql_repo}:9.1-aarch64 x86_64=${mysql_repo}:9.1 aarch64=${mysql_repo}:9.1-aarch64 amd64=${mysql_repo}:9.1)
    declare -r  hive_repo=erbou/metastore
    declare -ar hive_image=(arm64=${hive_repo}:4.0.1-aarch64 x86_64=${hive_repo}:4.0.1 aarch64=${hive_repo}:4.0.1-aarch64 amd64=${mysql_repo}:4.0.1)
    declare -r  workdir=.workdir
    declare -r  srcdir=$(dirname ${BASH_SOURCE-$0})
    declare -i  issues=0
    declare -i  in_docker=0

    if [[ -t 1 && -n "${TERM-}" ]]; then
        declare -r Re='\033[1;97;101m'
        declare -r Gr='\033[1;32m'
        declare -r Br='\033[1;90;103m'
        declare -r Nc='\033[0m'
    fi

    trap error ERR

    cd "${srcdir-.}"

    if ! command -v docker >& /dev/null; then
        in_docker=1
    fi

    while getopts 'hfcv:' vararg "${@:-}"; do
        case ${vararg} in
        h) usage; exit ;;
        f) cleanup ;;
        c) docker compose --env-file mysql.env --env-file minio.env config; exit ;;
        v) docker compose config --variables; exit;;
        esac
    done

    mkdir -m 0700 -p "${workdir}/config/"{trino,metastore,minio}

    if command -v rsync >& /dev/null; then
        rsync --ignore-existing -a config "${workdir}"/
        rsync -a "${workdir}"/config/trino/catalog "${workdir}"/config/trino/coordinator
        rsync -a "${workdir}"/config/trino/catalog "${workdir}"/config/trino/worker
        rsync -a "${workdir}"/config/trino/common/  "${workdir}"/config/trino/coordinator
        rsync -a "${workdir}"/config/trino/common/  "${workdir}"/config/trino/worker
    else
        cp -ur config "${workdir}/"
        cp -ur config/trino/catalog  "${workdir}/config/trino/coordinator"
        cp -ur config/trino/catalog  "${workdir}/config/trino/worker"
        cp -ur config/trino/common/* "${workdir}/config/trino/coordinator"
        cp -ur config/trino/common/* "${workdir}/config/trino/worker"
    fi

    gen_certs
    gen_mysql_env
    gen_minio_env
    gen_trino_env
    apply_templates
    export_certs

    set_env MYSQL_VER   "${mysql_version}"
    set_env MYSQL_IMAGE "$(echo ${mysql_image[*]}|sed -En 's|.*'$(uname -m)'=([^ ]+).*|\1|p')"
    set_env HIVE_IMAGE  "$(echo ${hive_image[*]}|sed -En 's|.*'$(uname -m)'=([^ ]+).*|\1|p')"

    if (( in_docker == 0 )); then
        if (( `docker info  -f '{{json .MemTotal }}'` < 8000000000 )); then
            warn "At least 8g must be reserved for the docker engine: e.g. colima start --memory 8"
        fi
    else
        warn_ "Verify that at least 8g is reserved for the docker engine: e.g. colima start --memory 8"
    fi

    grep -iq 'change.me' minio.env && warn 'Edit your minio.env'
    grep -iq 'change.me' mysql.env && warn 'Edit your mysql.env'

    (( issues == 0 )) && info "Ready to run: docker compose -p trinohack up -d"
}

main "${@}"
