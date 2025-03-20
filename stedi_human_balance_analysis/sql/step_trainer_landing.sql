-- Define a new external table in the stedi schema for step trainer data
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
  `sensorreadingtime` BIGINT,     
  `serialnumber` STRING,          
  `distancefromobject` INTEGER    
)
-- Configure JSON processing using OpenX JSON SerDe
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Set JSON parsing properties
WITH SERDEPROPERTIES (
  'case.insensitive' = 'TRUE',        
  'mapping' = 'TRUE',              
  'dots.in.keys' = 'FALSE',          
  'ignore.malformed.json' = 'FALSE'  
)
-- Specify how data is stored and processed
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'      
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'   
-- Point to the S3 storage location

LOCATION 's3://stedi-project-lake-house/step_trainer/landing/'
-- Define table metadata

TBLPROPERTIES (
  'classification' = 'json'   
);