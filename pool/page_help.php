<?php
#
function dohelp($data)
{
 return '<h1>Helpless</h1>Helpless';
}
#
function show_help($menu, $name)
{
 gopage(NULL, 'dohelp', $menu, $name);
}
#
?>
