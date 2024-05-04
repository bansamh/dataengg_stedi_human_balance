import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714819087793 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1714819087793")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714819086269 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1714819086269")

# Script generated for node SQL Query
SqlQuery3896 = '''
select * from step join acc on step.sensorreadingtime=acc.timestamp;

'''
SQLQuery_node1714819092652 = sparkSqlQuery(glueContext, query = SqlQuery3896, mapping = {"step":StepTrainerTrusted_node1714819087793, "acc":AccelerometerTrusted_node1714819086269}, transformation_ctx = "SQLQuery_node1714819092652")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1714819140796 = glueContext.getSink(path="s3://stedi-human-balance/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1714819140796")
MachineLearningCurated_node1714819140796.setCatalogInfo(catalogDatabase="stedi_mb",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1714819140796.setFormat("json")
MachineLearningCurated_node1714819140796.writeFrame(SQLQuery_node1714819092652)
job.commit()