#!/bin/sh
#
p1()
{
 while true ; do
  read line
  if [ "$?" != "0" ] ; then
	return
  fi
#  echo "$line"
  if [ ! -f "$line" ] ; then
	echo "ERR: file not found: '$line'"
  else
	base="`echo "$line" | sed -e "s/\(^.*ckdb2014\).*$/\1/"`"
	rest="`echo "$line" | sed -e "s/^.*ckdb2014\(.*\).log$/\1/"`"
	m="${rest:0:2}"
	d="${rest:2:2}"
	h="${rest:4:2}"
	tz="`date -d"2014-$m-$d 00:00" +%z`"
	fix="`date -u -d"2014-$m-$d $h:00 $tz" +"%m%d%H"`"
	echo "mv '$line' '$base$fix.log2'"
	mv "$line" "$base$fix.log2"
  fi
 done
}
#
p2()
{
 while true ; do
  read line
  if [ "$?" != "0" ] ; then
	return
  fi
  nn="${line/log2/log}"
  echo "mv '$line' '$nn'"
  mv "$line" "$nn"
 done
}
#
if [ "$1" = "/" ] ; then
 dir="$1"
else
 dir="${1%%/}"
fi
#
ls $dir/ckdb20140*.log | p1
ls $dir/ckdb20140*.log2 | p2
