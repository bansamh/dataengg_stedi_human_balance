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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1714812386404 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1714812386404")

# Script generated for node Customer Curated
CustomerCurated_node1714812377598 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/customer/curated2/"], "recurse": True}, transformation_ctx="CustomerCurated_node1714812377598")

# Script generated for node Join with Customer Curated
SqlQuery315 = '''
select step.* from step join cust on step.serialNumber=cust.serialNumber;
'''
JoinwithCustomerCurated_node1714812422239 = sparkSqlQuery(glueContext, query = SqlQuery315, mapping = {"step":StepTrainerLanding_node1714812386404, "cust":CustomerCurated_node1714812377598}, transformation_ctx = "JoinwithCustomerCurated_node1714812422239")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714812438228 = glueContext.getSink(path="s3://stedi-human-balance/step_trainer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1714812438228")
StepTrainerTrusted_node1714812438228.setCatalogInfo(catalogDatabase="stedi_mb",catalogTableName="step_trainer_trusted2")
StepTrainerTrusted_node1714812438228.setFormat("json")
StepTrainerTrusted_node1714812438228.writeFrame(JoinwithCustomerCurated_node1714812422239)
job.commit()