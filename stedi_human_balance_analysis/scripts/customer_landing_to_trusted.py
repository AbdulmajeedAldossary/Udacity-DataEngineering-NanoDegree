import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions

# Extract job parameters from command-line arguments
parameters = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up Spark and Glue environments
spark_ctx = SparkContext()
glue_env = GlueContext(spark_ctx)
spark_session = glue_env.spark_session
current_job = Job(glue_env)
current_job.init(parameters['JOB_NAME'], parameters)

# Load customer data from S3 landing zone
# Node 1: Source data input (timestamp reflects March 19, 2025)
landing_data_node_1745107200000 = glue_env.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://stedi-project-lake-house/customer/landing/"],
        "recurse": True
    },
    format="json",
    format_options={"multiline": False},
    transformation_ctx="landing_data_node_1745107200000"
)

# Filter out customers who haven't consented to research sharing
# Node 2: Privacy filtering (incremented timestamp)
privacy_consent_node_1745107200001 = Filter.apply(
    frame=landing_data_node_1745107200000,
    f=lambda customer: customer["shareWithResearchAsOfDate"] != 0,
    transformation_ctx="privacy_consent_node_1745107200001"
)

# Save filtered data to the trusted zone in S3
# Node 3: Output to trusted zone (incremented timestamp)
trusted_data_node_1745107200002 = glue_env.write_dynamic_frame.from_options(
    frame=privacy_consent_node_1745107200001,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-lake-house/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": []
    },
    transformation_ctx="trusted_data_node_1745107200002"
)

# Complete the job execution
current_job.commit()