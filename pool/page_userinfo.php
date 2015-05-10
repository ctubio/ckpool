<?php
#
function blocksorder($a, $b)
{
 if ($b['blocks'] == $a['blocks'])
	return $b['diffacc'] - $a['diffacc'];
 else
	return $a['blocks'] - $b['blocks'];
}
#
function douserinfo($data, $user)
{
 $ans = getUserInfo($user);

 $pg = '<h1>Block Hall of Fame</h1>'.$pg;
 $pg .= "<table callpadding=0 cellspacing=0 border=0>\n";
 $pg .= "<tr class=title>";
 $pg .= "<td class=dl>User</td>";
 $pg .= "<td class=dr>Blocks</td>";
 $pg .= "<td class=dr>Diff</td>";
 $pg .= "</tr>\n";

 if ($ans['STATUS'] == 'ok')
 {
	$all = array();
	$count = $ans['rows'];
	for ($i = 0; $i < $count; $i++)
	{
		$all[] = array('blocks' => $ans['blocks:'.$i],
				'username' => $ans['username:'.$i],
				'diffacc' => $ans['diffacc:'.$i]);
	}
	usort($all, 'blocksorder');

	for ($i = 0; $i < $count; $i++)
	{
		if (($i % 2) == 0)
			$row = 'even';
		else
			$row = 'odd';

		$pg .= "<tr class=$row>";
		$un = htmlspecialchars($all[$i]['username']);
		$pg .= "<td class=dl>$un</td>";
		$bl = $all[$i]['blocks'];
		$pg .= "<td class=dr>$bl</td>";
		$diffacc = difffmt($all[$i]['diffacc']);
		$pg .= "<td class=dr>$diffacc</td>";
		$pg .= "</tr>\n";
	}
 }
 $pg .= "</table>\n";

 return $pg;
}
#
function show_userinfo($info, $page, $menu, $name, $user)
{
 gopage($info, NULL, 'douserinfo', $page, $menu, $name, $user);
}
#
?>
