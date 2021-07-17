#!/bin/sh -e

[ -z "$1" ] && echo Usage: $0 SUBJECT && exit 1

cd ca-pki || (echo Run ./scripts/make-ca.sh first! && exit 1)

subject="$1"
shift
commonName="$(echo $subject | awk -F = '{ print $NF; }')"

# openssl ecparam \
#     -genkey \
#     -name secp256k1 \
#     -param_enc named_curve \
#     -out $commonName-private.pem

openssl genrsa \
    -out $commonName-private.pem \
    2048

openssl req \
    -new \
    -subj "$subject" \
    -key $commonName-private.pem \
    -out $commonName.csr

openssl x509 \
    -req \
    -sha256 \
    -days 365 \
    -in $commonName.csr \
    -CA ca-cert.pem \
    -CAkey ca-private.pem \
    -CAcreateserial \
    -out $commonName-cert.pem

openssl x509 \
    -text \
    -in $commonName-cert.pem
