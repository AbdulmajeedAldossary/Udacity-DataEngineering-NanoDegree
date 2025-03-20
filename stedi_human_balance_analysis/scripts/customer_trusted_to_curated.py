import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue import DynamicFrame

# Custom function to execute SQL queries on DynamicFrames
def execute_sql_query(glue_ctx, sql_query, table_mapping, ctx_name) -> DynamicFrame:
    # Register each input frame as a temporary view
    for view_name, dynamic_frame in table_mapping.items():
        dynamic_frame.toDF().createOrReplaceTempView(view_name)
    # Execute the SQL query and convert result back to DynamicFrame
    query_result = spark.sql(sql_query)
    return DynamicFrame.fromDF(query_result, glue_ctx, ctx_name)

# Extract job arguments
job_params = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Set up Spark and Glue environments
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark = glue_ctx.spark_session
current_job = Job(glue_ctx)
current_job.init(job_params["JOB_NAME"], job_params)

# Node 1: Load accelerometer data from catalog (March 19, 2025 timestamp)
accel_source_node_1745107200000 = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accel_source_node_1745107200000"
)

# Node 2: Load trusted customer data from catalog
customer_trusted_node_1745107200001 = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node_1745107200001"
)

# Node 3: Join customer and accelerometer data using SQL
join_query = """
SELECT *
FROM trusted_customers tc
INNER JOIN accel_data ad
ON tc.email = ad.user
"""
joined_data_node_1745107200002 = execute_sql_query(
    glue_ctx,
    sql_query=join_query,
    table_mapping={"trusted_customers": customer_trusted_node_1745107200001, "accel_data": accel_source_node_1745107200000},
    ctx_name="joined_data_node_1745107200002"
)

# Node 4: Drop fields and remove duplicates with SQL
dedupe_query = """
SELECT DISTINCT customerName, email, phone, birthDay, serialNumber,
    registrationDate, lastUpdateDate, shareWithResearchAsOfDate,
    shareWithPublicAsOfDate, shareWithFriendsAsOfDate
FROM source_data
"""
cleaned_data_node_1745107200003 = execute_sql_query(
    glue_ctx,
    sql_query=dedupe_query,
    table_mapping={"source_data": joined_data_node_1745107200002},
    ctx_name="cleaned_data_node_1745107200003"
)

# Node 5: Write curated data to S3 and update catalog
curated_sink_node_1745107200004 = glue_ctx.getSink(
    path="s3://stedi-project-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="curated_sink_node_1745107200004"
)
# Configure catalog metadata and output format
curated_sink_node_1745107200004.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_curated")
curated_sink_node_1745107200004.setFormat("json")
# Write the cleaned data to the curated zone
curated_sink_node_1745107200004.writeFrame(cleaned_data_node_1745107200003)

# Finalize the job
current_job.commit()