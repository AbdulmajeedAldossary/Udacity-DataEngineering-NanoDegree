-- Define an external table for curated machine learning data
CREATE EXTERNAL TABLE `machine_learning_curated` (
  -- User identifier field
  `user` string COMMENT 'from deserializer',
  -- Acceleration measurements across three axes
  `x` double COMMENT 'from deserializer',
  `y` double COMMENT 'from deserializer',
  `z` double COMMENT 'from deserializer',
  -- Timestamp of sensor reading
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  -- Device identifier
  `serialnumber` string COMMENT 'from deserializer',
  -- Measured distance from object
  `distancefromobject` int COMMENT 'from deserializer'
)
-- Configure JSON parsing using OpenX JSON SerDe
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
-- Set up storage format specifications
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'      
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'   
-- Specify the data location in S3
LOCATION 's3://stedi-project-lake-house/ML/curated/'
-- Add table metadata properties
TBLPROPERTIES (
  'classification' = 'json',                                 
  'CreatedByJob' = 'Machine Learning Trusted to Curated',     
 'CreatedByJobRun' = '${jobRunId}',  -- Filled by pipeline at runtime
);