<?php
#
function no_api()
{
 echo 'nil';
 exit(0);
}
#
function show_api($page, $menu, $name, $user)
{
 $u = getparam('username', true);
 $api = getparam('api', true);
 $ans = getAtts($u, 'KAPIKey.str');
 if ($ans['STATUS'] != 'ok')
	no_api();
 if (!isset($ans['KAPIKey.str']))
	no_api();
 if ($ans['KAPIKey.str'] != $api)
	no_api();
 $ans = homeInfo($u);
 if ($ans === false)
	no_api();
 $rep = fldEncode($ans, 'lastbc', true);
 $rep .= fldEncode($ans, 'lastheight', false);
 $rep .= fldEncode($ans, 'currndiff', false);
 $rep .= fldEncode($ans, 'lastblock', false);
 $rep .= fldEncode($ans, 'lastblockheight', false);
 $rep .= fldEncode($ans, 'blockacc', false);
 $rep .= fldEncode($ans, 'blockerr', false);
 $rep .= fldEncode($ans, 'p_hashrate5m', false);
 $rep .= fldEncode($ans, 'p_hashrate1hr', false);
 $rep .= fldEncode($ans, 'u_hashrate5m', false);
 $rep .= fldEncode($ans, 'u_hashrate1hr', false);
 echo $rep;
 exit(0);
}
#
?>
