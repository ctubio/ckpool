<?php
#
function dohelp($data, $user)
{
 return '<h1>Helpless</h1>Helpless';
}
#
function show_help($menu, $name, $user)
{
 gopage(NULL, 'dohelp', $menu, $name, $user);
}
#
?>
