#!/bin/sh
#
fldsep="`echo -e '\x02'`"
#
dsp()
{
 cut -c4-
# echo
}
process()
{
 # <256
 len=${#1}
 oct="`printf '%03o' "$len"`"
 code="`printf "\\\\$oct"`"
 all="$code$zero$zero$zero$1"
 printf "$code\\0\\0\\000$1" | nc -U -w 1 /opt/ckdb/listener | dsp
}
#
addid()
{
 msg="1.newid.idname=$1${fldsep}idvalue=$2"
 process "$msg"
}
#
addid userid 7
