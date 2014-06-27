SET SESSION AUTHORIZATION 'postgres';

BEGIN transaction;

DO $$
DECLARE ver TEXT;
BEGIN

 UPDATE version set version='0.3' where vlock=1 and version='0.2';

 IF found THEN
  RETURN;
 END IF;

 SELECT version into ver from version
  WHERE vlock=1;

 RAISE EXCEPTION 'Wrong DB version - expect "0.2" - found "%"', ver;

END $$;

DROP TABLE sharesummary;

CREATE TABLE sharesummary (
    userid bigint NOT NULL,
    workername character varying(256) NOT NULL,
    workinfoid bigint NOT NULL,
    diffacc float NOT NULL,
    diffsta float NOT NULL,
    diffdup float NOT NULL,
    diffhi float NOT NULL,
    diffrej float NOT NULL,
    shareacc float NOT NULL,
    sharesta float NOT NULL,
    sharedup float NOT NULL,
    sharehi float NOT NULL,
    sharerej float NOT NULL,
    sharecount bigint NOT NULL,
    errorcount bigint NOT NULL,
    firstshare timestamp with time zone NOT NULL,
    lastshare timestamp with time zone NOT NULL,
    complete char NOT NULL,
    createdate timestamp with time zone NOT NULL,
    createby character varying(64) NOT NULL,
    createcode character varying(128) NOT NULL,
    createinet character varying(128) NOT NULL,
    modifydate timestamp with time zone NOT NULL,
    modifyby character varying(64) NOT NULL,
    modifycode character varying(128) NOT NULL,
    modifyinet character varying(128) NOT NULL,
    PRIMARY KEY (userid, workername, workinfoid)
);

DROP TABLE blocksummary;

CREATE TABLE workmarkers (
    markerid bigint NOT NULL,
    workinfoidend bigint NOT NULL,
    workinfoidstart bigint NOT NULL,
    description character varying(256) DEFAULT ''::character varying NOT NULL,
    createdate timestamp with time zone NOT NULL,
    createby character varying(64) DEFAULT ''::character varying NOT NULL,
    createcode character varying(128) DEFAULT ''::character varying NOT NULL,
    createinet character varying(128) DEFAULT ''::character varying NOT NULL,
    expirydate timestamp with time zone DEFAULT '6666-06-06 06:06:06+00',
    PRIMARY KEY (workinfoidstart)
);
CREATE UNIQUE INDEX workmarkersid ON workmarkers USING btree (markerid);

CREATE TABLE markersummary (
    markerid bigint NOT NULL,
    userid bigint NOT NULL,
    workername character varying(256) NOT NULL,
    diffacc float NOT NULL,
    diffsta float NOT NULL,
    diffdup float NOT NULL,
    diffhi float NOT NULL,
    diffrej float NOT NULL,
    shareacc float NOT NULL,
    sharesta float NOT NULL,
    sharedup float NOT NULL,
    sharehi float NOT NULL,
    sharerej float NOT NULL,
    sharecount bigint NOT NULL,
    errorcount bigint NOT NULL,
    firstshare timestamp with time zone NOT NULL,
    lastshare timestamp with time zone NOT NULL,
    complete char NOT NULL,
    createdate timestamp with time zone NOT NULL,
    createby character varying(64) NOT NULL,
    createcode character varying(128) NOT NULL,
    createinet character varying(128) NOT NULL,
    modifydate timestamp with time zone NOT NULL,
    modifyby character varying(64) NOT NULL,
    modifycode character varying(128) NOT NULL,
    modifyinet character varying(128) NOT NULL,
    PRIMARY KEY (markerid, userid, workername)
);

ALTER TABLE ONLY eventlog
  ADD COLUMN poolinstance character varying(256) NOT NULL;

ALTER TABLE ONLY auths
  ADD COLUMN poolinstance character varying(256) DEFAULT ''::character varying NOT NULL;

ALTER TABLE ONLY auths
  ALTER COLUMN poolinstance DROP DEFAULT;

ALTER TABLE ONLY userstats
  ADD COLUMN workername character varying(256) NOT NULL;

ALTER TABLE ONLY poolstats
  ALTER COLUMN elapsed DROP DEFAULT;

END transaction;
