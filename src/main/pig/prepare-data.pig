REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;

raw = LOAD '$INPUT' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, recordid:chararray, length:long, headers:chararray, content:chararray, plaintext:chararray);

udfout = FOREACH raw GENERATE recordid, url, 
org.muehleisen.hannes.naward2014.Pr0nTagFinder(content) as headerpr0nflag, 
org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) as domainpr0nflag,
org.muehleisen.hannes.naward2014.CountryBlackMagic(ip,url,content) as country,
plaintext;

-- OR domainpr0nflag
mahoutin = FOREACH udfout GENERATE CONCAT(CONCAT('/',(headerpr0nflag ? 'porn':'clean')),CONCAT('/',recordid)), plaintext;
-- one percent sample for training purposes
trainingset = SAMPLE mahoutin 0.5;

STORE trainingset INTO '$OUTPUT/trainingset' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
'-c com.twitter.elephantbird.pig.util.TextConverter',
'-c com.twitter.elephantbird.pig.util.TextConverter'
);

STORE udfout INTO '$OUTPUT/fullset' USING BinStorage();

