SET SESSION AUTHORIZATION 'postgres';

BEGIN transaction;

DO $$
DECLARE ver TEXT;
BEGIN

 UPDATE version set version='0.4' where vlock=1 and version='0.3';

 IF found THEN
  RETURN;
 END IF;

 SELECT version into ver from version
  WHERE vlock=1;

 RAISE EXCEPTION 'Wrong DB version - expect "0.3" - found "%"', ver;

END $$;

ALTER TABLE ONLY userstats
  ADD COLUMN summarylevel char DEFAULT ' ' NOT NULL;

ALTER TABLE ONLY userstats
  ALTER COLUMN summarylevel DROP DEFAULT;

END transaction;
