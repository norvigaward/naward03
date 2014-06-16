REGISTER '/export/scratch2/hannes/naward03/target/naward2014-0.0.1-SNAPSHOT.jar'; 
register '/export/scratch2/hannes/naward03/lib/babel2012-1.0-SNAPSHOT.jar';

dta = LOAD '/export/scratch2/hannes/warcs/results' AS (url,headerpr0nflag:boolean,iplocation,domainpr0nflag:boolean,terms);


illustrate dta;

-- split/sample/wordcount
--langs = FOREACH html GENERATE url, nl.cwi.ins1.norvigaward.LangGuesser(html) AS lang;
