SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS stocksinput;
CREATE EXTERNAL TABLE stocksinput
(
	Calendardate string,
	Close DECIMAL(5,2),
	Volume DECIMAL(10,2),
	Open DECIMAL(5,2),
	High DECIMAL(5,2),
	Low DECIMAL(5,2),
	Ticker string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:inputtable}';

DROP TABLE IF EXISTS stockspartitioned; 
create external table stockspartitioned (  
	Calendardate string,
	Close DECIMAL(5,2),
	Volume DECIMAL(10,2),
	Open DECIMAL(5,2),
	High DECIMAL(5,2),
	Low DECIMAL(5,2),
	Ticker string
)
partitioned by ( yearno int, monthno int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '10' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:partitionedtable}';

DROP TABLE IF EXISTS Stage; 
CREATE TABLE IF NOT EXISTS Stage
(
	Calendardate string,
	Close DECIMAL(5,2),
	Volume DECIMAL(10,2),
	Open DECIMAL(5,2),
	High DECIMAL(5,2),
	Low DECIMAL(5,2),
	Ticker string,
	yearno int,
	monthno int

	 ) ROW FORMAT delimited fields terminated by ',' LINES TERMINATED BY '10';

INSERT OVERWRITE TABLE Stage
SELECT
Calendardate,
Close,
	Volume,
	Open,
	High,
	Low,
	Ticker,
  	year(Calendardate) as yearno,
  	month(Calendardate) as monthno
FROM stocksinput
WHERE Year(Calendardate) = ${hiveconf:Year} AND Month(Calendardate) = ${hiveconf:Month}; 

INSERT INTO TABLE stockspartitioned PARTITION( yearno , monthno) 
SELECT
Calendardate,
Close,
	Volume,
	Open,
	High,
	Low,
	Ticker,
  	yearno,
  	monthno
FROM Stage
WHERE YearNo = ${hiveconf:Year} AND MonthNo = ${hiveconf:Month};