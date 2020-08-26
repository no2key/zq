#!/bin/bash

# This file simulates a running version of brim desktop. It forks a zqd process
# then sits forever on the main thread.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. $DIR/awaitfile.sh

mkdir -p zqdroot
zqdroot=zqdroot
tempdir=$(mktemp -d)

mockbrim -zqddata="$zqdroot" -portfile="$tempdir/port" -pidfile="$tempdir/pid" &
brimpid=$!

# wait for zqd to start
awaitfile $tempdir/port
awaitfile $tempdir/pid

export ZQD_HOST=localhost:$(cat $tempdir/port)
export ZQD_PID=$(cat $tempdir/pid)
export BRIM_PID=$brimpid

# ensure that zqd process isn't leaked
trap "kill -9 $ZQD_PID 2>/dev/null" EXIT
rm -rf $tempdir
