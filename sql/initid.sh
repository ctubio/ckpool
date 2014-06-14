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
# Default to yyyymmddXXXXXX
# thus on reinit it will always be above old values
# XXXXXX should allow enough per day to avoid overlap with other ids
#  but of course overlapping with another id doesn't technically matter
now="`date +%Y%m%d`"
#
addid userid ${now}100000
addid workerid ${now}200000
addid paymentid ${now}300000
addid authid ${now}400000
