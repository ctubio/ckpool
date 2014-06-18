<?php
#
include_once('socket.php');
include_once('base.php');
#
global $send_sep, $fld_sep, $val_sep;
$send_sep = '.';
$fld_sep = Chr(0x2);
$val_sep = '=';
#
function myhash($str)
{
 return strtolower(hash('sha256', $str));
}
#
function repDecode($rep)
{
 global $send_sep, $fld_sep, $val_sep;

 $fix = preg_replace("/[\n\r]*$/",'',$rep);
 $major = explode($send_sep, $fix, 4);
 if (count($major) < 3)
	return false;

 $ans = array();
 if (count($major) > 3)
 {
	$flds = explode($fld_sep, $major[3]);
	foreach ($flds as $fld)
	{
		if (strlen($fld) > 0)
		{
			$nameval = explode($val_sep, $fld, 2);
			if (count($nameval) > 1)
				$ans[$nameval[0]] = $nameval[1];
			else
				$ans[$nameval[0]] = '';
		}
	}
 }

 $ans['ID'] = $major[0];
 $ans['STAMP'] = $major[1];
 $ans['STATUS'] = $major[2];

 return $ans;
}
#
function msgEncode($cmd, $id, $fields)
{
 global $send_sep, $fld_sep, $val_sep;

 $msg = $cmd . $send_sep . $id;
 $first = true;
 foreach ($fields as $name => $value)
 {
	if ($first === true)
	{
		$msg .= $send_sep;
		$first = false;
	}
	else
		$msg .= $fld_sep;

	$msg .= $name . $val_sep . $value;
 }
 return $msg;
}
#
function getStats($user)
{
 global $fld_sep;
 if ($user === null)
	$msg = msgEncode('homepage', 'home', array());
 else
	$msg = msgEncode('homepage', 'home', array('username'=>$user));
 return $msg;
}
#
function homeInfo($user)
{
 $msg = getStats($user);
 $rep = sendsockreply('homepage', $msg);
 if ($rep === false)
	$ans = false;
 else
 {
	$ans = repDecode($rep);
	$ans['lastblock'] = 1401237522;
 }

 return $ans;
}
#
function checkpass($user, $pass)
{
 $passhash = myhash($pass);
 $flds = array('username' => $user, 'passwordhash' => $passhash);
 $msg = msgEncode('chkpass', 'log', $flds);
 $rep = sendsockreply('checkpass', $msg);
 if (!$rep)
	dbdown();
 return $rep;
}
#
function getWorkers()
{
 list($who, $whoid) = validate();
 if ($who == false)
	showIndex();
 $flds = array('username' => $who);
 $msg = msgEncode('workers', 'work', $flds);
 $rep = sendsockreply('getworkers', $msg);
 if (!$rep)
	dbdown();
 return $rep;
}
#
function getPayments()
{
 list($who, $whoid) = validate();
 if ($who == false)
	showIndex();
 $flds = array('username' => $who);
 $msg = msgEncode('payments', 'pay', $flds);
 $rep = sendsockreply('getpayments', $msg);
 if (!$rep)
	dbdown();
 return $rep;
}
#
?>
