SET default_parallel 20;

SET mapreduce.map.memory.mb 8192;
SET mapred.max.map.failures.percent 10;

REGISTER lib/ia-porky-jar-with-dependencies.jar;

DEFINE FROMJSON org.archive.porky.FromJSON();
DEFINE SequenceFileLoader org.archive.porky.SequenceFileLoader();

WordCounts = LOAD '$I_WORD_COUNTS' AS (year:int, month:int, word:chararray, count:int, afterlast:int, url:chararray);

GroupedCounts = GROUP WordCounts BY (year, month, url, word);

AggregatedCounts = FOREACH GroupedCounts GENERATE
    group.year AS year, group.month AS month, group.url AS url, group.word AS word,
    SUM(WordCounts.count) as count;

STORE AggregatedCounts INTO '$O_DATA_DIR';
