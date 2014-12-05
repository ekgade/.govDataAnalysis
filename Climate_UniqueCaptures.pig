-- Climate Change Pig Script: Flags all captures whose content includes at least one mention of the term climate change and stores the
output
-- For questions contact ekgade@uw.edu

-- TO RUN = type this into the command line after having logged in the altiscale cluster with your ssh key: pig -p I_PARSED_DATA=/dataset-derived/gov/parsed/arcs/bucket-2/DOTGOV-EXTRACTION-1995-FY2013-MIME-TEXT-ARCS-PART-00999-000003.arc.gz -p I_CHECKSUM_DATA=/dataset/gov/url-ts-checksum/ -p O_DATA_DIR=1DecTest 1Dec_fullCounts_crisistoo.pig
-- make sure that your file paths are in the right place and that you start in the right directory (it doesn't give you clear errors abo
ut this) If you want to run on a single arc or warc file, the above parsed_data path will work - to run on a whole bucket you need to remove everything after the "bucket-2"

-- These first four lines are defaults and also help with memory (if you dont have them, sometimes the cluster kicks you out)
SET default_parallel 100;

SET mapreduce.map.memory.mb 8192;
SET mapred.max.map.failures.percent 10;
REGISTER lib/ia-porky-jar-with-dependencies.jar;

-- This is how you would call out a to a python script with a designated function if you wanted to
-- I used this for RegEx matching but its much slower so I switched to the long "or" statement you see below. However, if you are more comfortable with
--User defined functions in Python than this is what you do
REGISTER 'climatechange.py' USING jython AS myfuncs;

-- This allows you to use json functions in pig (important! many are underwritten in json)
DEFINE FROMJSON org.archive.porky.FromJSON();

-- The sequence file loader pulls the files out of the ARC/WARC format and makes them readable. Note, when they were put into the arc/warc format, the were run through a HTML parser to remove the
-- HTML boilerplate. However, if the file that you are interested in was not in HTML format to begin with, the parser will have just spit out goblydygock and this won't fix it. you have to deal with those issues seperately
DEFINE SequenceFileLoader org.archive.porky.SequenceFileLoader();

-- This flips the URL back to front so the important parts are at the beginning e.g. gov.whitehouse.frontpage......
-- I haven't really figured out why this is helpful, but it does help with using existing scripts because the ones internet archvie wrote use this format
DEFINE SURTURL org.archive.porky.SurtUrlKey();

-- when you load data, you have to use the same "name" for the data that you do in the command line command - so this is the name of the directory or file
-- that you want to run this script on
Archive = LOAD '$I_PARSED_DATA' USING SequenceFileLoader() AS (key:chararray, value:chararray);

-- generating the m# fields helps process the crazy .gz format into fields you can recognize (e.g. title; content)
Archive = FOREACH Archive GENERATE FROMJSON(value) AS m:[];

-- this drops any files that return an error message
Archive = FILTER Archive BY m#'errorMessage' is null;

-- this is saying for each value and key pair, pull out the following fields.
ExtractedCounts = FOREACH Archive GENERATE m#'url' AS src:chararray,
-- Chararray just means character array - so a list of characters with no limits on what can be in there
           SURTURL(m#'url') AS surt:chararray,
           REPLACE(m#'digest','sha1:','') AS checksum:chararray,
-- This selects the first eight characters of the date string (year, month, day) -- I did this because the (year, month, day, hour, second) format
-- is confusing for a lot of time formats down the line - python, postgresql, etc.
           SUBSTRING(m#'date', 0, 8) AS date:chararray,
-- I kept getting unicode errors in the below fields, so I found a regular expression that means "all printed charactures"
-- e.g. NOT new lines, carriage returns, etc. SO. this finds anything that is not text, punctuation and white space, and replaces it with a space
           REPLACE(m#'code', '[^\\p{Graph}]', ' ')          AS code:chararray,
           REPLACE(m#'title', '[^\\p{Graph}]', ' ')         AS title:chararray,
           REPLACE(m#'description', '[^\\p{Graph}]', ' ')   AS description:chararray,
           REPLACE(m#'content', '[^\\p{Graph}]', ' ')       AS content:chararray;

-- This takes each of the previous fields (the url, date, content, etc.) and searches
-- through the content field looking for any RegEx matches to these terms
-- If it finds one, it keeps it; otherwise "filter" drops the file
UniqueCaptures = FILTER ExtractedCounts BY content MATCHES '.*natural\\s+disaster.*' OR content MATCHES '.*desertification.*' OR content MATCHES '.*climate\\s+change.*' OR content MATCHES '.*pollution.*' OR content MATCHES '.*ocean\\s+acidification.*' OR content MATCHES '.*anthropocene.*' OR content MATCHES '.*anthropogenic.*' OR content MATCHES '.*greenhouse\\s+gas.*' OR content MATCHES '.*climategate.*' OR content MATCHES '.*climatic\\s+research\\s+unit.*' OR content MATCHES '.*security\\s+of\\s+food.*' OR content MATCHES '.*global\\s+warming.*' OR content MATCHES '.*fresh\\s+water.*' OR content MATCHES '.*forest\\s+conservation.*' OR content MATCHES '.*food\\s+security.*';

-- If you then wanted to further filter those terms by another set of terms, for example, you wanted a page that mentioned global warming but also talked about it
-- as a "threat" or a "crisis"... You could run this bit:

UniqueCaptures = FILTER UniqueCaptures BY content MATCHES '.*threat.*' OR content MATCHES '.*crisis.*' OR content MATCHES '.*security.*' OR content MATCHES '.*calamity.*' OR content MATCHES '.*catastroph.*' OR content MATCHES '.*disaster.*';

-- If you wanted to output counts instead of the full text of the page and its fields that have been selected for you.......
UniqueCaptures = FOREACH UniqueCaptures GENERATE myfuncs.pickURLs(src),
                                         src AS src,
                                         surt AS surt,
                                         checksum AS checksum,
                                         date AS date,
                                         myfuncs.Threat_countWords(content);

-- I ended up moving this to the front! Get rid of the \n (new line deliminaters) which are causing problems with tables - you can't change the document delimiter
in Pig, and the default is '\n' new line so we have to get rid of all the new lines in the text (tables and also some character limits). this will affect our abli
ty to do text parsing by paragraph, but sentences will still be okay.
-- UniqueCaptures = FOREACH UniqueCaptures GENERATE REPLACE(content, '\n', ' ');

-- to get TOTAL number of counts, rather than simply unique observations, merge with checksum data
Checksum = LOAD '$I_CHECKSUM_DATA' USING PigStorage() AS (surt:chararray, date:chararray, checksum:chararray);

CountsJoinChecksum = JOIN UniqueCaptures BY (surt, checksum), Checksum BY (surt, checksum);

FullCounts = FOREACH CountsJoinChecksum GENERATE
                         UniqueCaptures::src as src,
                         Checksum::date as date,
                         UniqueCaptures::counts as counts,
                         UniqueCaptures::URLs as URLs;

-- This would sort counts by original "source"or URL
GroupedCounts = GROUP FullCounts BY src;

--This fills in the missing counts
GroupedCounts = FOREACH GroupedCounts GENERATE
      group AS src,
      FLATTEN(myfuncs.fillInCounts(FullCounts)) AS (year:int, month:int, word:chararray, count:int, filled:int, afterlast:int, URLs:chararray);
STORE GroupedCounts INTO '$O_DATA_DIR';

-- This stores the counts the file name you gave it
-- The "using pigstorage" function allows you to set your own delimiters.
-- I chose one with Unicode because I was worried commas/tabs would show up in the existing text (obviously). And, since I stripped out all unicode above, this should be clearly a new field
--STORE UniqueCaptures INTO '$O_DATA_DIR' USING PigStorage('\u0001');
