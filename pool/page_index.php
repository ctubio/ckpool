<?php
#
function doindex($data)
{
 $pg = '
<h1>CKPool</h1>
Welcome to CKPool the bestest mostest gnarliest poolest in the ...... south.
';
 return $pg;
}
#
function show_index($menu, $name)
{
 gopage(NULL, 'doindex', $menu, $name);
}
#
?>
