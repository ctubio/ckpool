SET SESSION AUTHORIZATION 'postgres';

BEGIN transaction;

DO $$
DECLARE ver TEXT;
BEGIN

 UPDATE version set version='0.2' where vlock=1 and version='0.1';

 IF found THEN
  RETURN;
 END IF;

 SELECT version into ver from version
  WHERE vlock=1;

 RAISE EXCEPTION 'Wrong DB version - expect "0.1" - found "%"', ver;

END $$;

ALTER TABLE ONLY poolstats
    ADD COLUMN elapsed bigint DEFAULT 0 NOT NULL;

CREATE TABLE userstats (
    poolinstance character varying(256) NOT NULL,
    userid bigint NOT NULL,
    elapsed bigint DEFAULT 0 NOT NULL,
    hashrate float NOT NULL,
    hashrate5m float NOT NULL,
    hashrate1hr float NOT NULL,
    hashrate24hr float NOT NULL,
    createdate timestamp with time zone NOT NULL,
    createby character varying(64) DEFAULT ''::character varying NOT NULL,
    createcode character varying(128) DEFAULT ''::character varying NOT NULL,
    createinet character varying(128) DEFAULT ''::character varying NOT NULL,
    PRIMARY KEY (poolinstance, userid, createdate)
);

END transaction;
