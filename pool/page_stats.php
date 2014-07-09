<?php
#
function allusersort($a, $b)
{
 $cmp = $a['u_hashrate1hr'] != $b['u_hashrate1hr'];
 if ($cmp != 0)
	return $cmp;
 return $a['userid'] - $b['userid'];
}
#
function dostats($data, $user)
{
 $pg = '<h1>Pool Stats</h1>';

 $rep = getAllUsers();
 $ans = repDecode($rep);

 $pg .= "<table callpadding=0 cellspacing=0 border=0>\n";
 $pg .= "<tr class=title>";
 $pg .= "<td class=dl>Username</td>";
 $pg .= "<td class=dr>Hash Rate</td>";
 $pg .= "</tr>\n";
 if ($ans['STATUS'] == 'ok')
 {
	$all = array();
	$count = $ans['rows'];
	for ($i = 0; $i < $count; $i++)
	{
		$all[] = array('username' => $ans['username'.$i],
				'userid' => $ans['userid'.$i],
				'u_hashrate1hr' => $ans['u_hashrate1hr'.$i]);
	}

	usort($all, 'allusersort');

	foreach ($all as $arr)
	for ($i = 0; $i < $count; $i++)
	{
		if (($i % 2) == 0)
			$row = 'even';
		else
			$row = 'odd';

		$pg .= "<tr class=$row>";
		$pg .= '<td class=dl>'.$all[$i]['username'].'</td>';
		$uhr = $all[$i]['u_hashrate1hr'];
		if ($uhr == '?')
			$uhr = '?GHs';
		else
		{
			$uhr /= 10000000;
			if ($uhr < 100000)
				$uhr = (round($uhr)/100).'GHs';
			else
				$uhr = (round($uhr/1000)/100).'THs';
		}
		$pg .= "<td class=dr>$uhr</td>";
		$pg .= "</tr>\n";
	}
 }
 $pg .= "</table>\n";

 return $pg;
}
#
function show_stats($menu, $name, $user)
{
 gopage(NULL, 'dostats', $menu, $name, $user);
}
#
?>
