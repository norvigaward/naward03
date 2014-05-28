REGISTER '/export/scratch2/hannes/naward03/target/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;
SET pig.temp.dir '/export/scratch2/hannes/warcs/tmp';

raw = LOAD '/export/scratch2/hannes/warcs/CC-MAIN-20131218055411-00062-ip-10-33-133-15.ec2.internal.warc.gz' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, length:long, headers:chararray, content:chararray);

a = LIMIT raw 10000;

x = FOREACH a GENERATE url, 
	org.muehleisen.hannes.naward2014.PornTagFinder(content) as headerpornflag, 
	org.muehleisen.hannes.naward2014.IpGeoLocation(ip) as iplocation,
	org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) as domainpornflag;--,
	--org.muehleisen.hannes.naward2014.TokenizeStemStopfilter(content) AS terms;

p = FILTER x BY headerpornflag == true || domainpornflag = true;

DUMP p;

-- 

--training = SAMPLE raw 0.1;

-- first, look in tag or blacklist
-- second, use classifier
-- find payleveldomain
-- lookup geoloc