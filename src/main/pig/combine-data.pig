REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;

raw = LOAD '$INPUT/fullset' USING BinStorage() AS (recordid,url,ip, headerpr0nflag,domainpr0nflag,plaintext);
classout = FOREACH raw GENERATE recordid,url,headerpr0nflag,domainpr0nflag,org.muehleisen.hannes.naward2014.ApplyClassifier(plaintext,'$INPUT/model','$INPUT/labelindex','$INPUT/trainingset-seq/dictionary.file-*','$INPUT/trainingset-seq/df-count') as class,org.muehleisen.hannes.naward2014.CountryBlackMagic(ip,url,plaintext) as country;
STORE classout INTO '$OUTPUT/final' USING PigStorage('\\t');

classaggr = FOREACH classout GENERATE url,country,(headerpr0nflag OR domainpr0nflag OR class=='porn' ? true : false) as dirty;

by_country = GROUP classaggr BY (country, dirty);
country_clean_counts = FOREACH by_country GENERATE
    FLATTEN(group) AS (country, dirty),
    COUNT(classaggr) AS urls;

-- todo: finalclass should include the flags!
STORE country_clean_counts INTO '$OUTPUT/mapinput' USING PigStorage('\\t');
