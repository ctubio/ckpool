<?php
#
function doworkers($data, $user)
{
 $pg = '<h1>Workers</h1>';

 $rep = getWorkers($user);
 $ans = repDecode($rep);

 $pg .= "<table callpadding=0 cellspacing=0 border=0>\n";
 $pg .= "<tr class=title>";
 $pg .= "<td class=dl>Name</td>";
 $pg .= "<td class=dl>Difficulty</td>";
 $pg .= "<td class=dr>Idle Notifications</td>";
 $pg .= "<td class=dr>Idle Notification Time</td>";
 $pg .= "</tr>\n";
 if ($ans['STATUS'] == 'ok')
 {
	$count = $ans['rows'];
	for ($i = 0; $i < $count; $i++)
	{
		if (($i % 2) == 0)
			$row = 'even';
		else
			$row = 'odd';

		$pg .= "<tr class=$row>";
		$pg .= '<td class=dl>'.$ans['paydate'.$i].'</td>';
		$pg .= '<td class=dl>'.$ans['payaddress'.$i].'</td>';
		$pg .= '<td class=dr>'.btcfmt($ans['amount'.$i]).'</td>';
		$pg .= "</tr>\n";
	}
 }
 $pg .= "</table>\n";

 return $pg;
}
#
function show_workers($menu, $name, $user)
{
 gopage(NULL, 'doworkers', $menu, $name, $user);
}
#
?>
