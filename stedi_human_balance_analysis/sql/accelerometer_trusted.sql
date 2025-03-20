-- Define a new external table for trusted accelerometer data
CREATE EXTERNAL TABLE `accelerometer_trusted` (

  `user` string COMMENT 'from deserializer',

  `timestamp` bigint COMMENT 'from deserializer',
 
  `x` double COMMENT 'from deserializer',
 
  `y` double COMMENT 'from deserializer',
  
  `z` double COMMENT 'from deserializer'
)
-- Configure JSON serialization/deserialization
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Set up storage format specifications
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'     
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'   

-- Specify the data location in S3
LOCATION 's3://stedi-project-lake-house/accelerometer/trusted/'

-- Add table metadata properties
TBLPROPERTIES (
  'CreatedByJob' = 'Accelerometer Landing to Trusted',
  'CreatedByJobRun' = '${jobRunId}',  -- Filled by pipeline at runtime
  'classification' = 'json'
);