<?php
#
include_once('email.php');
#
function app_txt($ones)
{
 $app = "The free and recommended $ones that ";
 $app .= "have been tested here are:<br><span class=hil>";
 $app .= "Android: Google Play '<b>FreeOTP Authenticator</b>' by Red Hat<br>";
 $app .= "Apple: App Store '<b>OTP Auth</b>' by Roland Moers</span><br><br>";
 return $app;
}
#
function set_2fa($data, $user, $tfa, $ans, $err)
{
 $draw = false;

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
	$pg .= "You don't have Two Factor Authentication (2FA) setup yet<br><br>";
	$pg .= 'To use 2FA you need an App on your phone/tablet<br>';
	$pg .= app_txt('ones');
	$pg .= 'Click here to begin the setup process for 2FA: ';
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
		$draw = true;
		addQR();
	}
	else
	{
		$key = 'unavailable';
		$sfainfo = 'unavailable';
		$sfaurl = 'unavailable';
	}
	$pg .= "Your <span class=urg>2FA Secret Key</span> is: $key<br>";
	$pg .= "2FA Settings are $sfainfo<br><br>";
	$pg .= "To setup 2FA in your App: <a href='$sfaurl'>Click here</a><br>";
	$pg .= "or scan the qrcode/barcode below with your App:<br><br>";
	$pg .= '<div id=can0><canvas id=can width=1 height=1>';
	$pg .= 'A qrcode will show here if your browser supports html5/canvas';
	$pg .= "</canvas></div><br>";
	$pg .= 'Then enter your App 2FA Value: <input name=Value value="" size=10> ';
	$pg .= '<input type=submit name=Test value=Test></td></tr>';
	$pg .= '<tr class=dl><td>';
	$pg .= app_txt('2FA apps');
	$pg .= '<span class=urg>N.B.</span> if you wish to setup 2FA on more than one device,<br>';
	$pg .= 'you should setup all devices before testing one of them.<br>';
	$pg .= 'If you have an old <span class=urg>2FA Secret Key</span> in your device for this web site,<br>';
	$pg .= 'delete it before scanning in the new <span class=urg>2FA Secret Key</span>.<br><br>';
	$pg .= '<span class=urg>WARNING:</span> if you lose your 2FA device you will need to know<br>';
	$pg .= 'the <span class=urg>2FA Secret Key</span> to manually setup a new device,<br>';
	$pg .= 'so your should copy it and store it somewhere securely.<br>';
	$pg .= 'For security reasons, the site will not show you an active <span class=urg>2FA Secret Key</span>.<br>';
	$pg .= '</td></tr>';
	break;
  case 'ok':
	$pg .= '<tr class=dc><td>';
	$pg .= '2FA is enabled on your account.<br><br>';
	$pg .= 'If you wish to replace your Secret Key with a new one:<br><br>';
	$pg .= 'Current 2FA Value: <input name=Value value="" size=10> ';
	$pg .= '<input type=submit name=New value=New><span class=st1>*</span><br><br>';
	$pg .= '<span class=st1>*</span>WARNING: replacing the Secret Key will disable 2FA<br>';
	$pg .= 'until you successfully test the new key,<br>';
	$pg .= 'thus getting a new key is effectively the same as disabling 2FA.<br><br>';
	$pg .= '</td></tr>';
	break;
 }
 $pg .= '</table></form>';
 $pg .= '</center></td></tr>';

 $pg .= '<tr class=dl><td>';
 $pg .= '2FA means that you need 2 codes to login to your account.<br>';
 $pg .= 'You will also need the 2FA code to modify any important settings in your account.<br>';
 $pg .= 'The 1st code is your current password.<br>';
 $pg .= 'The 2nd code is a number that your 2FA device will generate each time.<br>';
 $pg .= 'Your 2FA device would be, for example, your phone or tablet.<br><br>';
 $pg .= 'Each time you need a 2FA code, you use your device to generate a number<br>';
 $pg .= 'that you type into the "<span class=st1>*</span>2nd Authentication:" field on any page that has it.<br><br>';
 $pg .= '<b>IMPORTANT:</b> the TOTP algorithm uses the time on your device,<br>';
 $pg .= "so it is important that your device's clock is accurate within a few seconds.<br><br>";
 $pg .= '<b>IMPORTANT:</b> you enter the value from your App at the time you submit data.<br>';
 $pg .= "The value is valid only once for a maximum of 30 seconds.<br>";
 $pg .= "In both the Apps it has a 'dial' that shows the 30 seconds running out.<br>";
 $pg .= "If you are close to running out, you can wait for the 30 seconds to run out<br>";
 $pg .= "and then enter the new value it will come up with.<br>";
 $pg .= "The pool checks your value using the time at the pool when you submit the data,<br>";
 $pg .= "it doesn't matter when you loaded the web page,<br>";
 $pg .= "it only matters when you clicked on the web page button to send the data to the pool.<br><br>";
 $pg .= '<span class=urg>WARNING:</span> once you have successfully tested and enabled 2FA,<br>';
 $pg .= 'you will be unable to access or even reset your account without 2FA.<br>';
 $pg .= 'There is no option to recover your 2FA from the web site,<br>';
 $pg .= 'and you must know your 2FA code in order to be able to disable 2FA.<br><br>';
 $pg .= '<span class=urg>WARNING:</span> it is important to <b>not</b> store your login password in your 2FA device.<br>';
 $pg .= 'These 2 together will give full access to your account.';
 $pg .= '</td></tr>';

 $pg .= '</table>';

 if ($draw !== false)
 {
	$qr = shell_exec("../pool/myqr.sh '$sfaurl'");
	if ($qr !== null and strlen($qr) > 30)
	{
		$pg .= "<script type='text/javascript'>\n";
		$pg .= "${qr}qr(tw,fa,qrx,qry,qrd);</script>\n";

		if (strpos($qr, 'var tw=1,fa=0,qrx=') === false)
			error_log("QR error for '$user' res='$qr'");
	}
	else
	{
		if ($qr === null)
			$qr = 'null';
		error_log("QR failed for '$user' res='$qr'");
	}
 }

 return $pg;
}
#
function do2fa($data, $user)
{
 $err = '';
 $setup = getparam('Setup', false);
 $testemail = false;
 if ($setup === 'Setup')
 {
	// rand() included as part of the entropy
	$ans = get2fa($user, 'setup', rand(1073741824,2147483647), 0);
	$testemail = true;
 }
 else
 {
	$value = getparam('Value', false);
	$test = getparam('Test', false);
	if ($test === 'Test' and $value !== null)
	{
		$ans = get2fa($user, 'test', 0, $value);
		$testemail = true;
	}
	else
	{
		$nw = getparam('New', false);
		if ($nw === 'New' and $value !== null)
		{
			$ans = get2fa($user, 'new', rand(1073741824,2147483647), $value);
			$testemail = true;
		}
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

	if ($testemail and $err == '')
	{
		$ans2 = userSettings($user);
		if ($ans2['STATUS'] != 'ok')
			dbdown(); // Should be no other reason?
		if (!isset($ans2['email']))
			$err = 'An error occurred, check your details below';
		else
		{
			$email = $ans2['email'];
			$emailinfo = getOpts($user, emailOptList());
			if ($emailinfo['STATUS'] != 'ok')
				$err = 'An error occurred, check your details below';
			else
			{
				if ($setup === 'Setup')
					twofaSetup($email, zeip(), $emailinfo);
				else if ($test === 'Test')
					twofaEnabled($email, zeip(), $emailinfo);
				else if ($nw === 'New')
					twofaSetup($email, zeip(), $emailinfo);
			}
		}
	}
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
