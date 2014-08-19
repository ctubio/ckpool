SET SESSION AUTHORIZATION 'postgres';

BEGIN transaction;

DO $$
DECLARE ver TEXT;
BEGIN

 UPDATE version set version='0.7' where vlock=1 and version='0.6';

 IF found THEN
  RETURN;
 END IF;

 SELECT version into ver from version
  WHERE vlock=1;

 RAISE EXCEPTION 'Wrong DB version - expect "0.6" - found "%"', ver;

END $$;

ALTER TABLE ONLY sharesummary
  ADD COLUMN lastdiffacc float DEFAULT 0;

ALTER TABLE ONLY sharesummary
  ALTER COLUMN lastdiffacc DROP DEFAULT;

END transaction;
