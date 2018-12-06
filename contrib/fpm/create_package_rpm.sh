#!/bin/bash -x

NAME=bioyino

die() {
    if [[ $1 -eq 0 ]]; then
        rm -rf "${TMPDIR}"
    else
        [ "${TMPDIR}" = "" ] || echo "Temporary data stored at '${TMPDIR}'"
    fi
    echo "$2"
    exit $1
}

pwd

GIT_VERSION="$(git describe --always --tags)" || die 1 "Can't get latest version from git"

set -f; IFS='-' ; arr=($GIT_VERSION)
VERSION=${arr[0]}; [ -z "${arr[2]}" ] && RELEASE=${arr[1]} || RELEASE=${arr[1]}.${arr[2]}
set +f; unset IFS

[ "${VERSION}" = "" -o  "${RELEASE}" = "" ] && {
	echo "Revision: ${RELEASE}";
	echo "Version: ${VERSION}";
	echo
	echo "Known tags:"
	git tag
	echo;
	die 1 "Can't parse version from git";
}

cargo build --release || die 1 "Build error"
strip target/release/bioyino || die 1 "Strip error"

TMPDIR=$(mktemp -d)
[ "${TMPDIR}" = "" ] && die 1 "Can't create temp dir"
echo version ${VERSION} release ${RELEASE}
mkdir -p "${TMPDIR}/usr/bin" || die 1 "Can't create bin dir"
mkdir -p "${TMPDIR}/usr/share/${NAME}" || die 1 "Can't create share dir"
mkdir -p "${TMPDIR}/etc/sysconfig" || die 1 "Can't create sysconfig dir"
mkdir -p "${TMPDIR}/etc/systemd/system" || die 1 "Can't create systemd dir"
cp ./target/release/bioyino "${TMPDIR}/usr/bin/" || die 1 "Can't install package binary"
cp ./config.toml "${TMPDIR}/usr/share/${NAME}" || die 1 "Can't install package shared files"
cp ./contrib/rhel/${NAME}.service "${TMPDIR}/etc/systemd/system" || die 1 "Can't install package systemd files"
cp ./contrib/common/${NAME}.env "${TMPDIR}/etc/sysconfig/${NAME}" || die 1 "Can't install package sysconfig file"

fpm -s dir -t rpm -n ${NAME} -v ${VERSION} -C ${TMPDIR} \
    --iteration ${RELEASE} \
    -p ${NAME}-VERSION-ITERATION.ARCH.rpm \
    --description "bioyino: high performance and high-precision multithreaded StatsD server" \
    --license BSD-2 \
    --url "https://github.com/avito-tech/bioyino" \
    --after-install contrib/fpm/post_install.sh \
    --post-uninstall contrib/fpm/post_uninstall.sh \
    "${@}" \
    etc usr/bin usr/share || die 1 "Can't create package!"

die 0 "Success"
