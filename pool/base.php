<?php
#
include_once('page.php');
#
global $dbg, $dbgstr;
$dbg = false;
$dbgstr = '';
#
function adddbg($str)
{
 global $dbg, $dbgstr;

 if ($dbg === true)
 {
	if ($dbgstr != '')
		$dbgstr .= "\n";
	$dbgstr .= $str;
 }
}
#
function btcfmt($amt)
{
 $amt /= 100000000;
 return number_format($amt, 8);
}
#
function emailStr($str)
{
 $all = '/[^A-Za-z0-9_+\.@-]/'; // no space = trim
 $beg = '/^[\.@+-]+/';
 $fin = '/[\.@+_-]+$/';
 return preg_replace(array($all,$beg,$fin), '', $str);
}
#
function loginStr($str)
{
 $all = '/[^!-~]/'; // no spaces
 return preg_replace($all, '', $str);
}
#
function trn($str)
{
 $rep = str_replace(array('<', '>'), array('&lt;', '&gt;'), $str);
 return $rep;
}
#
function htmler($str)
{
 $srch = array('<','>',"\r\n","\n","\r");
 $rep = array('&lt;','&gt;','<br>','<br>','<br>');
 return str_replace($srch, $rep, $str);
}
#
function cvtdbg()
{
 global $dbg, $dbgstr;

 if ($dbg === false || $dbgstr == '')
	$rep = '';
 else
	$rep = htmler($dbgstr).'<br>';

 return $rep;
}
#
function safeinput($txt, $len = 1024, $lf = true)
{
 $ret = trim($txt);
 if ($ret != '')
 {
	if ($lf === true)
		$ret = preg_replace("/[^ -~\r\n]/", '', $ret);
	else
		$ret = preg_replace('/[^ -~]/', '', $ret);

	if ($len > 0)
		$ret = substr($ret, 0, $len);
 }
 return trim($ret);
}
#
function safetext($txt, $len = 1024)
{
 $tmp = substr($txt, 0, $len);

 $res = '';
 for ($i = 0; $i < strlen($tmp); $i++)
 {
	$ch = substr($tmp, $i, 1);
	if ($ch >= ' ' && $ch <= '~')
		$res .= $ch;
	else
	{
		$c = ord($ch);
		$res .= sprintf('0x%02x', $c);
	}
 }

 if (strlen($txt) > $len)
	$res .= '...';

 return $res;
}
#
function dbd($data)
{
 return "<font color=red size=+10><br>Web site is currently down</font>";
}
#
function dbdown()
{
 gopage(NULL, 'dbd', NULL, '', true, false);
}
#
function f404($data)
{
 return "<font color=red size=+10><br>404</font>";
}
#
function do404()
{
 gopage(NULL, 'f404', NULL, '', true, false);
}
#
function showPage($page, $menu, $name)
{
# If you are doing development, use without '@'
# Then switch to '@' when finished
# @include_once("page_$page.php");
 include_once("page_$page.php");

 $fun = 'show_' . $page;
 if (function_exists($fun))
	$fun($menu, $name);
 else
	do404();
}
#
function showIndex()
{
 showPage('index', NULL, '');
}
#
function offline()
{
 if (file_exists('./maintenance.txt'))
 {
	$ip = $_SERVER['REMOTE_ADDR'];
	if ($ip != '192.168.7.74')
		gopage(NULL, file_get_contents('./maintenance.txt'), NULL, '', false, false);
 }
}
#
offline();
#
session_start();
#
include_once('db.php');
#
function validUserPass($user, $pass)
{
 $rep = checkpass($user, $pass);
 $ans = repDecode($rep);
 usleep(100000); // Max 10x per second
 if ($ans['STATUS'] == 'ok')
 {
	$key = 'ckp'.rand(1000000,9999999);
	$_SESSION['ckpkey'] = $key;
	$_SESSION[$key] = array('who' => $user, 'id' => $user);
 }
}
#
function logout()
{
 if (isset($_SESSION['ckpkey']))
 {
	$key = $_SESSION['ckpkey'];

	if (isset($_SESSION[$key]))
		unset($_SESSION[$key]);

	unset($_SESSION['ckpkey']);
 }
}
#
function requestRegister()
{
 $reg = getparam('Register', false);
 if ($reg !== NULL)
 {
	logout();
	return true;
 }
 return false;
}
#
function tryLogInOut()
{
 // If already logged in, it will ignore User/Pass
 if (isset($_SESSION['ckpkey']))
 {
	$logout = getparam('Logout', false);
	if (!nuem($logout) && $logout == 'Logout')
		logout();
 }
 else
 {
	$user = getparam('User', false);
	if ($user !== NULL)
		$user = loginStr($user);
	if (nuem($user))
		return;

	$pass = getparam('Pass', false);
	if (nuem($pass))
		return;

	$login = getparam('Login', false);
	if (nuem($login))
		return;

	validUserPass($user, $pass);
 }
}
#
function validate()
{
 $who = '';
 $whoid = '';

 if (!isset($_SESSION['ckpkey']))
	return false;

 $key = $_SESSION['ckpkey'];
 if (!isset($_SESSION[$key]))
 {
	logout();
	return array(false, NULL);
 }

 if (!isset($_SESSION[$key]['who']))
 {
	logout();
	return array(false, NULL);
 }

 $who = $_SESSION[$key]['who'];

 if (!isset($_SESSION[$key]['id']))
 {
	logout();
	return array(false, NULL);
 }

 $whoid = $_SESSION[$key]['id'];

 return array($who, $whoid);
}
#
function loggedIn()
{
 list($who, $whoid) = validate();
 if ($who == false)
	return false;

 return true;
}
#
?>
