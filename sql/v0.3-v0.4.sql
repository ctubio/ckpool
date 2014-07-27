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

DROP TABLE userstats;

CREATE TABLE userstats (
    userid bigint NOT NULL,
    workername character varying(256) NOT NULL,
    elapsed bigint NOT NULL,
    hashrate float NOT NULL,
    hashrate5m float NOT NULL,
    hashrate1hr float NOT NULL,
    hashrate24hr float NOT NULL,
    summarylevel char NOT NULL,
    statsdate timestamp with time zone NOT NULL,
    createdate timestamp with time zone NOT NULL,
    createby character varying(64) DEFAULT ''::character varying NOT NULL,
    createcode character varying(128) DEFAULT ''::character varying NOT NULL,
    createinet character varying(128) DEFAULT ''::character varying NOT NULL,
    PRIMARY KEY (userid, workername, summarylevel, statsdate)
);

END transaction;
