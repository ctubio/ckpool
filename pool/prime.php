<?php
#
include_once('param.php');
include_once('base.php');
#
function process($p)
{
 $menu = array(
	'Home' => array(
		'Home' => ''
	),
	'Account' => array(
		'Workers' => 'workers',
		'Payments' => 'payments',
		'Settings' => 'settings'
	),
	'Pool' => array(
		'Stats' => 'stats'
	),
	'gap' => NULL,
	'Help' => array(
		'Help' => 'help'
	)
 );
 $page = '';
 $n = '';
 foreach ($menu as $item => $options)
	if ($options !== NULL)
		foreach ($options as $name => $pagename)
			if ($pagename === $p)
			{
				$page = $p;
				$n = " - $name";
			}

 if ($page === '')
	showPage('index', $menu, '');
 else
	showPage($page, $menu, $n);
}
#
function check()
{
 tryLogInOut();
 $in = loggedIn();
 if ($in == false)
 {
	if (requestRegister() == true)
		showPage('reg', NULL, '');
	else
		showIndex();
 }
 else
 {
	$p = getparam('k', true);
	process($p);
 }
}
#
check();
#
?>
