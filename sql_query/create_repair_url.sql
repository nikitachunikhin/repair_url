CREATE EXTERNAL TABLE IF NOT EXISTS mykyta.url_repair (
  id STRING,
  url STRING
)
PARTITIONED BY (
  startdate STRING,
  enddate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://dst-workbench/mykyta/repair_url/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");