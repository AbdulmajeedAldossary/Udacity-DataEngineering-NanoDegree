import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Custom function to execute SQL queries on DynamicFrames
def execute_sql_query(glue_ctx, sql_query, frame_mapping, ctx_name) -> DynamicFrame:
    # Register each frame as a temporary view for SQL
    for view_name, dynamic_frame in frame_mapping.items():
        dynamic_frame.toDF().createOrReplaceTempView(view_name)
    # Execute the SQL query and convert result back to DynamicFrame
    query_result = glue_ctx.spark_session.sql(sql_query)
    return DynamicFrame.fromDF(query_result, glue_ctx, ctx_name)

# Parse job arguments
job_params = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up Spark and Glue contexts
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark_session = glue_ctx.spark_session

# Initialize the Glue job
glue_job = Job(glue_ctx)
glue_job.init(job_params['JOB_NAME'], job_params)

# Node 1: Load step trainer data from S3 landing zone
# Using timestamp for March 19, 2025, 00:00:00 UTC (1745107200000)
step_trainer_source_node_1745107200000 = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://stedi-project-lake-house/step_trainer/landing/"],
        "recurse": True
    },
    format="json",
    format_options={"multiline": False},
    transformation_ctx="step_trainer_source_node_1745107200000"
)

# Node 2: Load curated customer data from Glue Data Catalog
customer_curated_node_1745107200001 = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node_1745107200001"
)

# Define SQL query for joining and filtering
join_filter_query = '''
SELECT st.sensorReadingTime, st.serialNumber, st.distanceFromObject
FROM customer_curated cus
INNER JOIN step_trainer_landing st
ON cus.serialNumber = st.serialNumber
'''

# Node 3: Join step trainer data with customer data and filter
joined_data_node_1745107200002 = execute_sql_query(
    glue_ctx,
    sql_query=join_filter_query,
    frame_mapping={
        "customer_curated": customer_curated_node_1745107200001,
        "step_trainer_landing": step_trainer_source_node_1745107200000
    },
    ctx_name="joined_data_node_1745107200002"
)

# Node 4: Write results to S3 trusted zone and update Glue catalog
trusted_sink_node_1745107200003 = glue_ctx.getSink(
    path="s3://stedi-project-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="trusted_sink_node_1745107200003"
)
# Configure catalog metadata and format
trusted_sink_node_1745107200003.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
trusted_sink_node_1745107200003.setFormat("json")
# Write the filtered data
trusted_sink_node_1745107200003.writeFrame(joined_data_node_1745107200002)

# Finalize and commit the job
glue_job.commit()