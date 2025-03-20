-- Define an external table for customer trusted data in 'stedi' schema if it doesn't exist

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_trusted` (
  -- Customer personal information
  `customerName` STRING,         
  `email` STRING,               
  `phone` STRING,               
  `birthDay` STRING,           
  
  -- Device and registration details
  `serialNumber` STRING,         
  `registrationDate` BIGINT,      
  `lastUpdateDate` BIGINT,       
  
  -- Data sharing permissions with timestamps
  `shareWithResearchAsOfDate` BIGINT,   
  `shareWithPublicAsOfDate` BIGINT,     
  `shareWithFriendsAsOfDate` BIGINT     
)
-- Configure JSON parsing using OpenX JSON SerDe
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive' = 'TRUE',       
  'mapping' = 'TRUE',             
  'ignore.malformed.json' = 'FALSE', 
  'dots.in.keys' = 'FALSE'           
)
-- Specify Hadoop file formats for reading and writing
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'     
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'   

-- Set the data source location in S3
LOCATION 's3://stedi-project-lake-house/customer/trusted/'

-- Define table metadata
TBLPROPERTIES (
  'classification' = 'json'     
);