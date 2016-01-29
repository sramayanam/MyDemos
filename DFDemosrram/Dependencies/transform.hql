DROP TABLE IF EXISTS stocksdata;
CREATE EXTERNAL TABLE stocksdata
(
	Calendardate string,
	Close DECIMAL(5,2),
	Volume DECIMAL(10,2),
	Open DECIMAL(5,2),
	High DECIMAL(5,2),
	Low DECIMAL(5,2),
	Ticker string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:stocksdataInput}';
 
DROP TABLE IF EXISTS refdata;
CREATE EXTERNAL TABLE refdata
(
	Ticker string,
	Name string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:refdataInput}';

DROP TABLE IF EXISTS stocks;
CREATE EXTERNAL TABLE stocks
(
	Ticker string,
	Name string,
	Calendardate string,
	Close DECIMAL(5,2),
	Volume DECIMAL(10,2),
	Open DECIMAL(5,2),
	High DECIMAL(5,2),
	Low DECIMAL(5,2)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:stocksOutput}';

INSERT OVERWRITE TABLE stocks
SELECT 
	s.Ticker as Ticker,
	r.Name as Name,
	s.Calendardate as Calendardate,
	s.Close as Close,
	s.Volume as Volume,
	s.Open as Open,
	s.High as High,
	s.Low as Low
FROM stocksdata s
JOIN refdata r
ON (s.Ticker=r.Ticker);