#!/bin/bash

function awaitfile {
  file=$1
  i=0
  until [ -f $file ]; do
    let i+=1
    if [ $i -gt 5 ]; then
      echo "timed out waiting for file \"$file\" to appear"
      exit 1
    fi
    sleep 1
  done
}

function awaitsuccess {
  fn=$1
  i=0
  until $fn; do
    let i+=1
    echo "try.1"
    if [ $i -gt 5 ]; then
      echo "timed out waiting for fn to exit successfully" 
      exit 1
    fi
    sleep 1
  done
}
