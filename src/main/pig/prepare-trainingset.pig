REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 
REGISTER '$LIBS/babel2012-1.0-SNAPSHOT.jar';

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;

--/export/scratch2/hannes/warcs/CC-MAIN-20131218055411-00062-ip-10-33-133-15.ec2.internal.warc.gz
--/export/scratch2/hannes/warcs/IAH-20080430204825-00000-blackbook.warc.gz
raw = LOAD '$INPUT' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, recordid:chararray, length:long, headers:chararray, content:chararray, plaintext:chararray);

udfout = FOREACH raw GENERATE recordid, url, 
org.muehleisen.hannes.naward2014.Pr0nTagFinder(content) as headerpr0nflag, 
org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) as domainpr0nflag,
org.muehleisen.hannes.naward2014.IpGeoLocation(ip) as iplocation,
org.muehleisen.hannes.naward2014.ccTldFinder(ip) as tldlocation,
UPPER(nl.cwi.ins1.norvigaward.LangGuesser(content)) as language,
plaintext;


-- make sure we get a reasonable size of porn pages in there. but how?
sampleout = SAMPLE udfout 0.1;
trainingset = FOREACH sampleout GENERATE CONCAT(CONCAT('/',(headerpr0nflag OR domainpr0nflag ? 'porn':'clean')),CONCAT('/',recordid)), plaintext;
explain trainingset;
STORE trainingset INTO '$OUTPUT' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
'-c com.twitter.elephantbird.pig.util.TextConverter',
'-c com.twitter.elephantbird.pig.util.TextConverter'
);



-- -- STORE udfout INTO '/export/scratch2/hannes/warcs/transformed' USING BinStorage();

-- udfout = LOAD '/export/scratch2/hannes/warcs/transformed' USING BinStorage() AS (url,headerpr0nflag,domainpr0nflag,iplocation,tldlocation,language,
-- 	terms);

-- training = SAMPLE udfout 0.1;

-- termstraining = FOREACH training GENERATE url, headerpr0nflag as pr0nflag, FLATTEN(terms) AS term;
-- by_flag_term = GROUP termstraining BY (pr0nflag, term);
-- by_url_term = GROUP termstraining BY (url, term);


-- gres = FOREACH by_flag_term GENERATE
--     FLATTEN(group) AS (pr0nflag, term),
--     COUNT(termstraining) AS term_count, LOG(COUNT(termstraining)) as term_count_log;

-- gres = FOREACH by_url_term GENERATE
--     FLATTEN(group) AS (pr0nflag, term),
--     COUNT(termstraining) AS document_frequency, LOG(COUNT(termstraining)) as term_count_log;
-- grest = FILTER gres BY term_count > 10 AND pr0nflag;
-- --gresto = ORDER grest BY term_count DESC;

-- terms = FOREACH udfout GENERATE url, FLATTEN(terms) AS term;

-- aa = JOIN terms BY term, gres BY term;
-- bb = GROUP aa BY (url, pr0nflag);
-- cc = FOREACH bb GENERATE
--     FLATTEN(group) AS (url, pr0nflag),
--     SUM(aa.term_count_log) AS term_count_log_sum;

-- DUMP cc;


--SPLIT udfout INTO pr0no IF (headerpr0nflag OR domainpr0nflag), clean IF NOT (headerpr0nflag AND domainpr0nflag);

--pr0no = FILTER udfout BY headerpr0nflag AND domainpr0nflag;
--clean = FILTER udfout BY NOT (headerpr0nflag OR domainpr0nflag);

--pr0nosample = SAMPLE pr0no 0.01;
--cleansample = SAMPLE clean 0.01;



--illustrate pr0nosample;

--STORE x INTO '/export/scratch2/hannes/warcs/results';

--p = FILTER x BY headerpr0nflag == true;
--DUMP p;
--STORE x INTO '/export/scratch2/hannes/warcs/results';

-- 

--training = SAMPLE raw 0.1;

-- first, look in tag or blacklist
-- second, use classifier
-- find payleveldomain
-- lookup geoloc

-- SET pig.temp.dir '/export/scratch2/hannes/warcs/tmp/';
-- SET hadoop.tmp.dir '/export/scratch2/hannes/warcs/tmp/';