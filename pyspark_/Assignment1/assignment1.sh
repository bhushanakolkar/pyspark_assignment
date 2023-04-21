#!/bin/bash  

a=$(pwd)

mkdir -p $a/tmp/output

python3.7 $1 -f $2 -d $a/tmp/output 
