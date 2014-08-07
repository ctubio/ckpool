select 'sharesummary' as "sharesummary",min(firstshare) as "min incomplete firstshare" from sharesummary where complete = 'n';
select 'workinfo' as "workinfo",max(createdate) as "max createdate" from workinfo;
select 'auths' as "auths",max(createdate) as "max createdate" from auths;
select 'poolstats' as "poolstats",max(createdate) as "max createdate" from poolstats;
select 'userstats' as "userstats",max(statsdate) as "max statsdate - start of this hour" from userstats;
