REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapreduce.map.failures.maxpercent 10;


raw = LOAD '$INPUT' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, recordid:chararray, length:long, headers:chararray, content:chararray, plaintext:chararray);

ti = LIMIT raw 100;

-- this is the big one
classout = FOREACH ti GENERATE url,
 (org.muehleisen.hannes.naward2014.Pr0nTagFinder(content) or 
 	org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) or 
 	org.muehleisen.hannes.naward2014.USC2257Finder(plaintext) ? 'porn':'clean') as class1:chararray, 
    org.muehleisen.hannes.naward2014.ApplyClassifier(plaintext) as class2, 
 --'clean' as class2,
 org.muehleisen.hannes.naward2014.CountryBlackMagic(ip,url,plaintext) as country;

DUMP classout;

-- STORE classout INTO '$OUTPUT/final' USING PigStorage('\\t');

-- classaggr = FOREACH classout GENERATE url,country,(class1 == 'porn' OR class2=='porn' ? true : false) as dirty;

-- by_country = GROUP classaggr BY (country, dirty);
-- country_clean_counts = FOREACH by_country GENERATE
--     FLATTEN(group) AS (country, dirty),
--     COUNT(classaggr) AS urls;

-- -- todo: finalclass should include the flags!
-- STORE country_clean_counts INTO '$OUTPUT/mapinput' USING PigStorage('\\t');


