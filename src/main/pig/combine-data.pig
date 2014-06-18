REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapred.max.map.failures.percent 10;

raw = LOAD '$INPUT/fullset' 
USING BinStorage()
AS (recordid,url,headerpr0nflag,domainpr0nflag,country,plaintext);

classout = FOREACH raw GENERATE recordid,url,headerpr0nflag,domainpr0nflag,country,org.muehleisen.hannes.naward2014.ApplyClassifier(plaintext,'$INPUT/model','$INPUT/labelindex','$INPUT/trainingset-seq/dictionary.file-0','$INPUT/trainingset-seq/df-count') as class;

STORE classout INTO '$OUTPUT/final' USING PigStorage('\\t');