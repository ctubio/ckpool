<?php
#
function dousperf($data, $user)
{
 $ans = getShiftData($user);

 $iCrap = strpos($_SERVER['HTTP_USER_AGENT'],'iP');
 if ($iCrap)
	$ok = 0;
 else
	$ok = 1;

 $pg = '<h1>User Shift Performance</h1><br>';
 if ($ans['STATUS'] == 'ok' and $ans['DATA'] != '')
 {
	$pg .= "<div id=can0><canvas id=can width=1 height=1>";
	$pg .= "A graph will show here if your browser supports html5/canvas";
	$pg .= "</canvas></div>\n";
	$data = str_replace(array("\\","'"), array("\\\\","\\'"), $ans['DATA']);
	$pg .= "<script src='/can.js'></script>\n";
	$pg .= "<script type='text/javascript'>dodrw($ok,'$data');</script>\n";
 }

 return $pg;
}
#
function show_usperf($info, $page, $menu, $name, $user)
{
 gopage($info, NULL, 'dousperf', $page, $menu, $name, $user);
}
#
?>
