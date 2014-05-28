REGISTER '/export/scratch2/hannes/naward03/target/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;
SET pig.temp.dir '/export/scratch2/hannes/warcs/tmp/';
SET hadoop.tmp.dir '/export/scratch2/hannes/warcs/tmp/';


raw = LOAD '/export/scratch2/hannes/warcs/CC-MAIN-20131218055411-00062-ip-10-33-133-15.ec2.internal.warc.gz' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, length:long, headers:chararray, content:chararray);

rawsm = FILTER raw BY length < 1048576;
a = LIMIT rawsm 10000;

x = FOREACH a GENERATE url, 
	org.muehleisen.hannes.naward2014.Pr0nTagFinder(content) as headerpr0nflag, 
	org.muehleisen.hannes.naward2014.IpGeoLocation(ip) as iplocation,
	org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) as domainpr0nflag;--,
	--org.muehleisen.hannes.naward2014.TokenizeStemStopfilter(content) AS terms;

p = FILTER x BY headerpr0nflag == true;
DUMP p;
--STORE x INTO '/export/scratch2/hannes/warcs/results';

-- 

--training = SAMPLE raw 0.1;

-- first, look in tag or blacklist
-- second, use classifier
-- find payleveldomain
-- lookup geoloc