#!/bin/bash

# HOWTO Multiplatform image build
#
# - Create a docker-container buildx driver:
#   - docker buildx create --name container_name --driver=docker-container --driver-opt=[key=value,...] container_endpoint
#   - Ref:
#     - https://docs.docker.com/build/builders/
#     - https://docs.docker.com/build/builders/manage/
#     - https://docs.docker.com/build/building/multi-platform/
#     - https://docs.docker.com/build/drivers/docker-container/
# - docker buildx build --builder=container_name --tag repository:tag --platform linux/amd64,linux/arm64 .
# - Or: ./build.sh arm amd -r repository

declare -r tag=${IMAGE_TAG-4.0.1}
declare -r rep=${IMAGE_REPOSITORY-hive}
declare arch=

main() {
    set -euCE
    local -a build_opts=()

    while (( ${#@} > 0 )); do
        case ${1} in
        p|-p|plain|--plain) build_opts+=(--progress=plain) ;;
        j|-j|json|--json)   build_opts+=(--progress=rawjson) ;;
        amd|-amd|--amd) build_opts+=(--platform linux/amd64); tagarch=${tag} ;;
        arm|-arm|--arm) build_opts+=(--platform linux/arm64); tagarch=${tag}-aarch64 ;;
        r|-r|--repo) build_opts+=(--tag "${2}:${tagarch}"); shift ;;
        *) echo "$0 [-j|-p] [-arm|-amd] [-r repository]"; return ;;
        esac
        shift
    done

    [[ "${build_opts[*]-}" =~ --tag ]] || build_opts+=(--tag "${rep}:${tag}")

    docker buildx build "${build_opts[@]}" -f Dockerfile .
}

main $@
