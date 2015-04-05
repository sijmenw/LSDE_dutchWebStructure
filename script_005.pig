REGISTER 'hdfs:///user/hannesm/jwat/archive-commons-jar-with-dependencies.jar';

Links = LOAD '/data/public/common-crawl/crawl-data/CC-MAIN-2014-10/segments/*/wat/*.warc.wat.gz'
USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.WARC-Header-Metadata.WARC-Target-URI','Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.@Links.{url}')
AS (source:chararray,target:chararray);/**/

RemoteOnly = FILTER Links BY target MATCHES '^http(s)?://.*' ;
NLTarget = FILTER RemoteOnly BY target MATCHES '^http(s)?://[^/]+\\.nl(/.*|$)';
NLOnly = FILTER NLTarget BY source MATCHES '^http(s)?://[^/]+\\.nl(/.*|$)';

NLSourceGrouped = GROUP NLOnly BY source;
PageCounts = FOREACH NLSourceGrouped GENERATE group as source, COUNT(NLOnly) as count;
PageCountsGrouped = GROUP PageCounts by count;
Outdegree = FOREACH PageCountsGrouped GENERATE group as Outdegree, COUNT(PageCounts) as Occurences;
OutdegreeTop = ORDER PageCounts by count DESC;
OutdegreeTop20 = LIMIT OutdegreeTop 20;

NLTargetGrouped = GROUP NLOnly BY target;
PageCounts = FOREACH NLTargetGrouped GENERATE group as source, COUNT(NLOnly) as count;
PageCountsGrouped = GROUP PageCounts by count;
Indegree = FOREACH PageCountsGrouped GENERATE group as Indegree, COUNT(PageCounts) as Occurences;
IndegreeTop = ORDER PageCounts by count DESC;
IndegreeTop20 = LIMIT IndegreeTop 20;

STORE Indegree INTO 'output005_1';
STORE Outdegree INTO 'output005_2';
STORE IndegreeTop20 INTO 'output005_3';
STORE OutdegreeTop20 INTO 'output005_4';