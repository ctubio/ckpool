<?php
#
function dopayout($data, $user)
{
 return '<h1>Payouts</h1>Description';
}
#
function show_payout($menu, $name, $user)
{
 gopage(NULL, 'dopayout', $menu, $name, $user);
}
#
?>
