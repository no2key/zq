#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source $(DIR)/awaitfile.sh

zqdroot=$1
if [ -z "$zqdroot" ]; then
  zqdroot=zqdroot
  mkdir -p zqdroot
fi

mkdir -p s3/bucket
portdir=$(mktemp -d)


minio server --writeportfile="$portdir/minio" --quiet --address localhost:0 ./s3 &
miniopid=$!
awaitfile $portdir/minio

# AWS env variables must be set before zqd starts so zqd has access to them.
export AWS_REGION=does-not-matter
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_ENDPOINT=http://localhost:$(cat $portdir/minio)

zqd listen -l=localhost:0 -portfile="$portdir/zqd" -data="$zqdroot" -loglevel=warn &
zqdpid=$!
trap "rm -rf $portdir; kill -9 $miniopid $zqdpid" EXIT

awaitfile $portdir/zqd

export ZQD_HOST=localhost:$(cat $portdir/zqd)
