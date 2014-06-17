<?php
#
include_once('socket.php');
#
function doreg($data)
{
 if (isset($data['user']))
	$user = htmlspecialchars($data['user']);
 else
	$user = '';

 if (isset($data['mail']))
	$mail = htmlspecialchars($data['mail']);
 else
	$mail = '';

 $pg = '<h1>Register</h1>';
 if (isset($data['error']))
	$pg .= "<br><b>".$data['error']." - please try again</b><br><br>";
 $pg .= "
<form action=index.php method=POST>
<table>
<tr><td class=dr>Username:</td>
 <td class=dl><input name=user value=\"$user\"></td></tr>
<tr><td class=dr>Email:</td>
 <td class=dl><input name=mail value=\"$mail\"></td></tr>
<tr><td class=dr>Password:</td>
 <td class=dl><input type=password name=pass></td></tr>
<tr><td class=dr>Retype Password:</td>
 <td class=dl><input type=password name=pass2></td></tr>
<tr><td>&nbsp;</td>
 <td class=dl><input type=submit name=Register value=Register></td></tr>
<tr><td colspan=2 class=dc><br><font size=-1>All fields are required</font></td></tr>
</table>
</form>";

 return $pg;
}
#
function doreg2($data)
{
 $pg = '<h1>Registered</h1>';
 $pg .= '<br>You will receive an email shortly to verify your account';
 return $pg;
}
#
function safepass($pass)
{
 if (strlen($pass) < 6)
	return false;

 # Invalid characters
 $p2 = preg_replace('/[^ -~]/', '', $pass);
 if ($p2 != $pass)
	return false;

 # At least one lowercase
 $p2 = preg_replace('/[a-z]/', '', $pass);
 if ($p2 == $pass)
	return false;

 # At least one uppercase
 $p2 = preg_replace('/[A-Z]/', '', $pass);
 if ($p2 == $pass)
	return false;

 # At least one digit
 $p2 = preg_replace('/[0-9]/', '', $pass);
 if ($p2 == $pass)
	return false;

 return true;
}
#
function show_reg($menu, $name)
{
 $user = getparam('user', false);
 $mail = getparam('mail', false);
 $pass = getparam('pass', false);
 $pass2 = getparam('pass2', false);

 $data = array();
 $ok = true;
 if ($user === NULL && $mail === NULL && $pass === NULL && $pass2 === NULL)
	 $ok = false;
 else
 {
	if ($user !== NULL)
		$data['user'] = $user;
	else
		$ok = false;
	if ($mail !== NULL)
		$data['mail'] = $mail;
	else
		$ok = false;
	if ($pass === NULL || safepass($pass) !== true)
	{
		$ok = false;
		$data['error'] = "Password is unsafe";
	} elseif ($pass2 === NULL || $pass2 != $pass)
	{
		$ok = false;
		$data['error'] = "Passwords don't match";
	}
 }

 if ($ok === true)
 {
	$passhash = myhash($pass);
	$flds = array('username' => $user,
			'emailaddress' => $mail,
			'passwordhash' => $passhash);
	$msg = msgEncode('adduser', 'reg', $flds);
	$rep = sendsockreply('show_reg', $msg);
	if (!$rep)
		dbdown();

	$ans = repDecode($rep);
	if ($ans['STATUS'] == 'ok')
		gopage($data, 'doreg2', $menu, $name, true, true, false);
	else
		$data['error'] = "Invalid details";
 }

 gopage($data, 'doreg', $menu, $name, true, true, false);
}
#
?>
