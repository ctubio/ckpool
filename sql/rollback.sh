#!/bin/bash
#
# WARNING!!!
# 1) Don't use this
# 2) If you do use this, only use it if your "ckpool's ckdb logfiles"
#	(CCLs) are 100% certainly OK
#	Data in the DB will be lost if the rollback requires reloading
#	data that is missing from the CCLs
# 3) If you do use this, make sure ckdb is NOT running
# 4) The rollback is only for the tables that are generated from the CCLs
#	If you need data from other tables, see tabdump.sql
# 5) If you need to reload missing data that may or may not be in the CCLs
#	then you need to convince Kano to update the ckdb -y option to handle
#	that, with the current db, and also add code to the -y option to
#	update the DB
#	i.e. the ckdb -y option needs updating
#
# The basic use of this script is for when something goes wrong with the
#	database and the CCLs are OK - so you can roll back to before the
#	database problem and reload all the data
#
# Deleting markersummaries would only be an option if you know for sure
#	that all data is in the CCLs
# It would be ideal to do a markersummary table dump before a rollback
# However, -e will be OK if the CCLs aren't missing anything reqired for
#	the reload
#
# In all cases, if data is missing between the start and end of the
#  rollback, then the reload SEQ checking will report it, unless ...
#  by coincidence the missing data is at the end of a ckpool sequence
#  and no gaps exist in the data before the truncation point
# e.g. seqall goes from N..M then a new SEQ starts at 0..
#  however if there actually was data after M, but all missing exactly up
#  to before the SEQ restart 0 point, then ckdb couldn't know this unless
#  the data was inconsistent
#
# -e will of course allow you to compare all but markersummary, before and
#    after rolling back, by manually comparing the expired record with the
#    new replacement unexpired record - but it deletes the markersummary data
# -e is untidy simply because it leaves around unnecesary data, if the
#    reload after the rollback is not missing anything and succeeds
# -e deletes all the records that were already expired so you will lose
#    the original DB history of those records, however a reload should
#    regenerate a similar history
#
# -m is the same as -e except it doesn't touch markersummary at all
# -m will probably cause duplicate markersummary issues for ckdb
#
# -r ... use at your own risk :) - but is the best option if nothing is
#    missing from your CCLs
#    it simply deletes all reload data back to the specified point and
#    that data should all reappear with a successful reload
#
usAge()
{
 echo "usAge: `basename $0` [-n] -e|-r|-m workinfoid"
 echo " Preferred option is just -e as long as you aren't missing reload data"
 echo
 echo " -n = use 'now()' instead of the date timestamp of now, for expiry date"
 echo
 echo " -e = rollback by deleteing expired data and expiring unexpired data,"
 echo "      but deleting markersummary"
 echo
 echo " -r = rollback by deleting all relevant data (expired or unexpired)"
 echo " -m = rollback by deleteing expired data and expiring unexpired data,"
 echo "      but don't touch markersummary"
 echo "      N.B. -m is expected to cause problems with ckdb"
 echo
 echo " generates the sql on stdout"
 echo
 echo " Read `basename $0` for an explanation"
 exit 1
}
#
idctl()
{
 echo "\\echo 'update idcontrol $1'
update idcontrol
set lastid=(select max($1) from $2),
modifydate=$n,modifyby='$idn',
modifycode='$mc',modifyinet='$mi'
where idname='$1';"
}
#
process()
{
 # so all stamps are exactly the same
 now="`date +%s`"
 idn="rollback_${wi}_`date -d "@$now" '+%Y%m%d%H%M%S%:::z'`"
 mc="`basename $0`"
 mi="127.0.0.1"
 if [ "$usenow" ] ; then
	n="now()"
 else
	n="'`date -u -d "@$now" '+%Y-%m-%d %H:%M:%S+00'`'"
 fi
 ex="expirydate = $n"
 unex="expirydate > '6666-06-01'"
 oldex="expirydate < '6666-06-01'"
 #
 if [ "$opt" = "-r" -o "$opt" = "-e" ] ; then
	echo "\\echo 'delete markersummary'"
	echo "delete from markersummary where markerid in"
	echo " (select distinct markerid from workmarkers where workinfoidend >= $wi);"
 fi
 if [ "$opt" = "-r" ] ; then
	echo "\\echo 'delete marks'"
	echo "delete from marks where workinfoid >= $wi;"
	echo "\\echo 'delete workmarkers'"
	echo "delete from workmarkers where workinfoidend >= $wi;"
 else
	echo "\\echo 'delete/update marks'"
	echo "delete from marks where $oldex and workinfoid >= $wi;"
	echo "update marks set $ex where $unex and workinfoid >= $wi;"
	echo "\\echo 'delete/update workmarkers'"
	echo "delete from workmarkers where $oldex and workinfoidend >= $wi;"
	echo "update workmarkers set $ex where $unex and workinfoidend >= $wi;"
 fi
 #
 if [ "$opt" = "-r" ] ; then
	echo "\\echo 'delete blocks'"
	echo "delete from blocks where workinfoid >= $wi;"
	echo "\\echo 'delete workinfo'"
	echo "delete from workinfo where workinfoid >= $wi;"
 else
	echo "\\echo 'delete/update blocks'"
	echo "delete from blocks where $oldex and workinfoid >= $wi;"
	echo "update blocks set $ex where $unex and workinfoid >= $wi;"
	echo "\\echo 'delete/update workinfo'"
	echo "delete from workinfo where $oldex and workinfoid >= $wi;"
	echo "update workinfo set $ex where $unex and workinfoid >= $wi;"
 fi
 #
 if [ "$opt" = "-r" ] ; then
	echo "\\echo 'delete miningpayouts'"
	echo "delete from miningpayouts where payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "\\echo 'delete payments'"
	echo "delete from payments where payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "\\echo 'delete payouts'"
	echo "delete from payouts where workinfoidend >= $wi;"
 else
	echo "\\echo 'delete/update miningpayouts'"
	echo "delete from miningpayouts where $oldex and payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "update miningpayouts set $ex where $unex and payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "\\echo 'delete/update payments'"
	echo "delete from payments where $oldex and payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "update payments set $ex where $unex and payoutid in"
	echo " (select distinct payoutid from payouts where workinfoidend >= $wi);"
	echo "\\echo 'delete/update payouts'"
	echo "delete from payouts where $oldex and workinfoidend >= $wi;"
	echo "update payouts set $ex where workinfoidend >= $wi and $unex;"
 fi
 #
 idctl markerid workmarkers
 idctl paymentid payments
 idctl payoutid payouts
 # this makes sure the worker data is consistent - don't remove this
 idctl workerid workers
}
#
if [ -z "$1" ] ; then
 usAge
fi
#
if [ "$1" = "-?" -o "$1" = "-h" -o "$1" = "-help" -o "$1" = "--help" ] ; then
 usAge
fi
#
usenow=""
if [ "$1" = "-n" ] ; then
 usenow="y"
 shift
fi
#
if [ "$1" != "-r" -a "$1" != "-e" -a "$1" != "-m" ] ; then
 echo "ERR: Unknown p1='$1'"
 usAge
fi
#
opt="$1"
shift
#
if [ -z "$1" ] ; then
 echo "ERR: missing workinfoid"
 usAge
fi
#
wi="$1"
#
process
