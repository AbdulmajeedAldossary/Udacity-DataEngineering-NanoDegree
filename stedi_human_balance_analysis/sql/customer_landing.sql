-- Define an external table for customer data in the landing zone
CREATE EXTERNAL TABLE IF NOT EXISTS `customer_landing` (
  -- Customer identification fields
  `customername` STRING COMMENT 'Customer full name from JSON data',
  `email` STRING COMMENT 'Customer email address from JSON data',
  `phone` STRING COMMENT 'Customer phone number from JSON data',
  
  -- Personal and device information
  `birthday` STRING COMMENT 'Customer date of birth from JSON data',
  `serialnumber` STRING COMMENT 'Device serial number from JSON data',
  
  -- Key timestamp fields stored as bigint
  `registrationdate` BIGINT COMMENT 'Date of customer registration from JSON data',
  `lastupdatedate` BIGINT COMMENT 'Last update timestamp from JSON data',
  
  -- Data sharing consent timestamps
  `sharewithresearchasofdate` BIGINT COMMENT 'Research sharing consent date from JSON data',
  `sharewithpublicasofdate` BIGINT COMMENT 'Public sharing consent date from JSON data',
  `sharewithfriendsasofdate` BIGINT COMMENT 'Friends sharing consent date from JSON data'
)
-- Configure JSON serialization/deserialization
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

-- Specify Hadoop file formats for reading and writing
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'    
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'   

-- Set the S3 storage location for source data
LOCATION 's3://stedi-project-lake-house/customer/landing/'

-- Define table properties
TBLPROPERTIES (
  'classification' = 'json'  
);