
-- execute througth hue - hive
CREATE EXTERNAL TABLE COVID19 (
date DATE,country STRING,confirmed INT,recovered INT,deaths INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n' 
LOCATION '/user/hive/COVID19';
--tblproperties("skip.header.line.count"="1");



---------------------------------------------------------------------------
CREATE EXTERNAL TABLE COVID19LINES (
            age INT,
             sex STRING,
             city STRING,
             province  STRING,
             country  STRING ,
             latitude  INT ,
             longitude  INT ,
             date_admission_hospital DATE ,
             date_confirmation DATE,
             lives_in_Wuhan  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
LOCATION '/user/hive/COVID19LINES';
--tblproperties("skip.header.line.count"="1");






