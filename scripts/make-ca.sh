#!/bin/sh -e

[ -d ca-pki ] && echo CA already exists, exiting && exit 0

mkdir -p ca-pki && cd ca-pki

openssl genrsa \
    -out ca-private.pem \
    2048

openssl req \
    -new \
    -sha256 \
    -x509 \
    -days 3650 \
    -subj "${1:-/OU=mipt_distencode/CN=CA}" \
    -key ca-private.pem \
    -out ca-cert.pem

openssl x509 \
    -text \
    -in ca-cert.pem
