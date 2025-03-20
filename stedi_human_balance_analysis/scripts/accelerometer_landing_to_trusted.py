import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions

# Custom function to execute SQL queries on DynamicFrames
def execute_sql_query(glue_ctx, sql_query, source_mapping, ctx_name) -> DynamicFrame:
    # Create temporary views for each source frame
    for view_name, dyn_frame in source_mapping.items():
        dyn_frame.toDF().createOrReplaceTempView(view_name)
    # Execute SQL query and convert result back to DynamicFrame
    query_result = spark.sql(sql_query)
    return DynamicFrame.fromDF(query_result, glue_ctx, ctx_name)

# Get job parameters and initialize Spark/Glue context
job_args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark = glue_ctx.spark_session
glue_job = Job(glue_ctx)
glue_job.init(job_args['JOB_NAME'], job_args)

# Load source data from catalog - Customer Trusted
customer_trusted_frame = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_load"
)

# Load source data from catalog - Accelerometer Landing
accel_landing_frame = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accel_landing_load"
)

# Define SQL query for joining and filtering data
join_query = """
    SELECT 
        acc.user,
        acc.timestamp,
        acc.x,
        acc.y,
        acc.z
    FROM customer_trusted cust
    INNER JOIN accelerometer_landing acc
    ON cust.email = acc.user
"""

# Execute join operation between customer and accelerometer data
filtered_data = execute_sql_query(
    glue_ctx,
    sql_query=join_query,
    source_mapping={
        "customer_trusted": customer_trusted_frame,
        "accelerometer_landing": accel_landing_frame
    },
    ctx_name="privacy_filter_join"
)

# Configure and write output to S3 trusted zone
output_sink = glue_ctx.getSink(
    path="s3://stedi-project-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accel_trusted_output"
)
# Set catalog information for the output table
output_sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="accelerometer_trusted"
)
output_sink.setFormat("json")
output_sink.writeFrame(filtered_data)

# Finalize and commit the job
glue_job.commit()