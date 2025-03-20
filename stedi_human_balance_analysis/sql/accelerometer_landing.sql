-- Create an external table named 'accelerometer_landing' in the 'stedi' schema if it doesn't already exist

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
  `user` string,          
  `timestamp` bigint,     
  `x` float,            
  `y` float,            
  `z` float            
)
-- Specify the row format using JSON SerDe (Serializer/Deserializer) to handle JSON data

ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Define SerDe properties for JSON parsing
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',  -- Do not ignore malformed JSON (will throw errors if encountered)
  'dots.in.keys' = 'FALSE',          -- Do not allow dots in JSON keys
  'case.insensitive' = 'TRUE',       -- Make JSON key matching case-insensitive
  'mapping' = 'TRUE'                -- Enable mapping of JSON fields to table columns
)
-- Specify storage formats for input and output
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'    
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 

-- Define the S3 location where the source data resides

LOCATION 's3://stedi-project-lake-house/accelerometer/landing/'

-- Table properties to specify data format
TBLPROPERTIES (
  'classification' = 'json'  -- Indicate that the source data is in JSON format
);