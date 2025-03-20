-- Define an external table for trusted step trainer data
CREATE EXTERNAL TABLE `step_trainer_trusted` (
  `sensorreadingtime` BIGINT     
    COMMENT 'Extracted via deserializer from JSON',
  `serialnumber` STRING        
    COMMENT 'Extracted via deserializer from JSON',
  `distancefromobject` INTEGER  
    COMMENT 'Extracted via deserializer from JSON'
)
-- Configure JSON parsing using OpenX JSON SerDe
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Specify Hadoop file format handlers
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'      
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  

-- Set the S3 storage location for the table data
LOCATION 's3://stedi-project-lake-house/step_trainer/trusted/'

-- Define table metadata properties
TBLPROPERTIES (
  'classification' = 'json',                    - 
  'CreatedByJob' = 'Step Trainer Landing To Trusted',  
  'CreatedByJobRun' = '${jobRunId}'             
);