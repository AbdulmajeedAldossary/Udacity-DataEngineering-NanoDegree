-- Define an external table for curated customer data
CREATE EXTERNAL TABLE `customer_curated` (

  `customername` STRING COMMENT 'Customer full name from JSON deserializer',
  `email` STRING COMMENT 'Customer email address from JSON deserializer',
  `phone` STRING COMMENT 'Customer phone number from JSON deserializer',
  `birthday` STRING COMMENT 'Customer date of birth from JSON deserializer',
  
  -- Device and registration related fields
  `serialnumber` STRING COMMENT 'Device serial number from JSON deserializer',
  `registrationdate` BIGINT COMMENT 'Timestamp of customer registration from JSON deserializer',
  `lastupdatedate` BIGINT COMMENT 'Timestamp of last profile update from JSON deserializer',
  
  -- Data sharing permission timestamps
  `sharewithresearchasofdate` BIGINT COMMENT 'Research sharing consent timestamp from JSON deserializer',
  `sharewithpublicasofdate` BIGINT COMMENT 'Public sharing consent timestamp from JSON deserializer',
  `sharewithfriendsasofdate` BIGINT COMMENT 'Friends sharing consent timestamp from JSON deserializer'
)
-- Configure JSON serialization/deserialization
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Specify storage input/output formats
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'       
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  

-- Set the S3 bucket location for curated customer data
LOCATION 's3://stedi-project-lake-house/customer/curated/'

-- Add table metadata properties
TBLPROPERTIES (
  'CreatedByJob' = 'Customer Trusted to Curated',
  'CreatedByJobRun' = '${jobRunId}',  -- Filled by pipeline at runtime
  'classification' = 'json'
)