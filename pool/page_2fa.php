<?php
#
function set_2fa($data, $user, $tfa, $ans, $err)
{
 $pg = '<h1>Two Factor Authentication Settings</h1>';

 if ($err !== null and $err != '')
	$pg .= "<span class=err>$err<br><br></span>";

 $pg .= '<table cellpadding=20 cellspacing=0 border=1>';
 $pg .= '<tr class=dc><td><center>';

 $pg .= makeForm('2fa');
 $pg .= '<table cellpadding=5 cellspacing=0 border=0>';
 $pg .= '<tr class=dc><td>';
 switch ($tfa)
 {
  case '':
	$pg .= '<tr class=dl><td>';
	$pg .= "You don't have 2FA setup yet<br><br>";
	$pg .= 'To use 2FA you need an App on your phone/tablet<br>';
	$pg .= 'The free and recommended ones that have been tested here are:<br><br>';
	$pg .= "Android: Google Play 'FreeOTP Authenticator' by Red Hat<br>";
	$pg .= "Apple: App Store 'OTP Auth' by Roland Moers<br><br>";
	$pg .= 'Click here to start setting up 2FA: ';
	$pg .= '<input type=submit name=Setup value=Setup>';
	$pg .= '</td></tr>';
	break;
  case 'test':
	$pg .= '<tr class=dc><td>';
	$pg .= '2FA is not yet enabled.<br>';
	$pg .= 'Your 2FA key has been created but needs testing.<br><br>';
	if (isset($ans['2fa_key']))
	{
		$key = $ans['2fa_key'];
		$sfainfo = $ans['2fa_issuer'].': '.$ans['2fa_auth'].' '.
			   $ans['2fa_hash'].' '.$ans['2fa_time'].'s';
		$who = substr($user, 0, 8);
		$sfaurl = 'otpauth://'.$ans['2fa_auth'].'/'.$ans['2fa_issuer'].
			  ':'.htmlspecialchars($who).'?secret='.$ans['2fa_key'].
			  '&algorithm='.$ans['2fa_hash'].'&issuer='.$ans['2fa_issuer'];
	}
	else
	{
		$key = 'unavailable';
		$sfainfo = 'unavailable';
		$sfaurl = 'unavailable';
	}
	$pg .= "Your 2FA Secret Key is: $key<br>";
	$pg .= "2FA Settings are $sfainfo<br><br>";
	$pg .= "2FA URL is <a href='$sfaurl'>Click</a><br><br>";
	$pg .= '2FA Value: <input name=Value value="" size=10> ';
	$pg .= '<input type=submit name=Test value=Test>';
	$pg .= '</td></tr>';
	break;
  case 'ok':
	$pg .= '<tr class=dc><td>';
	$pg .= '2FA is enabled on your account.<br><br>';
	$pg .= 'If you wish to replace your Secret Key with a new one:<br><br>';
	$pg .= 'Current 2FA Value: <input name=Value value="" size=10> ';
	$pg .= '<input type=submit name=New value=New><span class=st1>*</span><br><br>';
	$pg .= '<span class=st1>*</span>WARNING: replacing the Secret Key will disable 2FA<br>';
	$pg .= 'until you successfully test the new key.<br><br>';
	$pg .= '</td></tr>';
	break;
 }

 $pg .= '</table></form>';

 $pg .= '</center></td></tr>';
 $pg .= '</table>';

 return $pg;
}
#
function do2fa($data, $user)
{
 $err = '';
 $setup = getparam('Setup', false);
 if ($setup === 'Setup')
 {
	// rand() included as part of the entropy
	$ans = get2fa($user, 'setup', rand(1073741824,2147483647), 0);
 }
 else
 {
	$value = getparam('Value', false);

	$test = getparam('Test', false);
	if ($test === 'Test' and $value !== null)
		$ans = get2fa($user, 'test', 0, $value);
	else
	{
		$nw = getparam('New', false);
		if ($nw === 'New' and $value !== null)
			$ans = get2fa($user, 'new', rand(1073741824,2147483647), $value);
		else
			$ans = get2fa($user, '', 0, 0);
	}
 }
 if ($ans['STATUS'] != 'ok')
	$err = 'DBERR';
 else
 {
	if (isset($ans['2fa_error']))
		$err = $ans['2fa_error'];
 }
 if (!isset($ans['2fa_status']))
	$tfa = null;
 else
	$tfa = $ans['2fa_status'];
 $pg = set_2fa($data, $user, $tfa, $ans, $err);
 return $pg;
}
#
function show_2fa($info, $page, $menu, $name, $user)
{
 gopage($info, NULL, 'do2fa', $page, $menu, $name, $user);
}
#
?>
