<?php
#
function allusersort($a, $b)
{
 $cmp = $b['u_hashrate5m'] - $a['u_hashrate5m'];
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
 $pg .= "<td class=dr>Hash Rate 5m</td>";
 $pg .= "</tr>\n";
 if ($ans['STATUS'] == 'ok')
 {
	$all = array();
	$count = $ans['rows'];
	for ($i = 0; $i < $count; $i++)
	{
		$all[] = array('username' => $ans['username'.$i],
				'userid' => $ans['userid'.$i],
				'u_hashrate5m' => $ans['u_hashrate5m'.$i]);
	}

	usort($all, 'allusersort');

	for ($i = 0; $i < $count; $i++)
	{
		if (($i % 2) == 0)
			$row = 'even';
		else
			$row = 'odd';

		$pg .= "<tr class=$row>";
		$pg .= '<td class=dl>'.$all[$i]['username'].'</td>';
		$uhr = $all[$i]['u_hashrate5m'];
		if ($uhr == '?')
			$dsp = '?GHs';
		else
		{
			$uhr /= 10000000;
			if ($uhr < 100000)
				$rate = 'G';
			else
			{
				$rate = 'T';
				$uhr /= 1000;
			}
			$dsp = number_format($urh/100, 2);
		}
		$pg .= "<td class=dr>$dsp</td>";
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
