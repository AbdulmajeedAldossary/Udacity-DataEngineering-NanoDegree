import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Custom function to execute SQL queries on DynamicFrames
def execute_sql_query(glue_ctx, sql_query, frame_mapping, ctx_name) -> DynamicFrame:
    # Register each DynamicFrame as a temporary SQL view
    for view_name, dyn_frame in frame_mapping.items():
        dyn_frame.toDF().createOrReplaceTempView(view_name)
    # Run the SQL query and convert result back to DynamicFrame
    query_result = spark.sql(sql_query)
    return DynamicFrame.fromDF(query_result, glue_ctx, ctx_name)

# Parse job arguments
job_params = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up Spark and Glue contexts
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark = glue_ctx.spark_session
glue_job = Job(glue_ctx)
glue_job.init(job_params['JOB_NAME'], job_params)

# Load accelerometer trusted data from Glue catalog
# Node 1: Using today's timestamp (March 19, 2025)
accel_trusted_node_1745107200000 = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accel_trusted_node_1745107200000"
)

# Load step trainer trusted data from Glue catalog
# Node 2: Incrementing timestamp slightly
step_trainer_node_1745107200001 = glue_ctx.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_node_1745107200001"
)

# Define SQL query to join accelerometer and step trainer data
join_query = """
SELECT acc.user, acc.x, acc.y, acc.z, 
       st.sensorreadingtime, st.serialnumber, st.distancefromobject
FROM accel_trusted acc
INNER JOIN step_trainer st
ON st.sensorreadingtime = acc.timestamp
"""

# Perform SQL join on the two datasets
# Node 3: Aggregated result with today's timestamp
joined_data_node_1745107200002 = execute_sql_query(
    glue_ctx,
    sql_query=join_query,
    frame_mapping={"accel_trusted": accel_trusted_node_1745107200000, "step_trainer": step_trainer_node_1745107200001},
    ctx_name="joined_data_node_1745107200002"
)

# Write joined data to S3 and update Glue catalog
# Node 4: Curated output with today's timestamp
ml_curated_sink_1745107200003 = glue_ctx.getSink(
    path="s3://stedi-project-lake-house/ML/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="ml_curated_sink_1745107200003"
)
# Configure catalog metadata for the curated table
ml_curated_sink_1745107200003.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
ml_curated_sink_1745107200003.setFormat("json")
ml_curated_sink_1745107200003.writeFrame(joined_data_node_1745107200002)

# Commit the job to finalize execution
glue_job.commit()