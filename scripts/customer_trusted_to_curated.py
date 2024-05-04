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

# Script generated for node Customer Trusted
CustomerTrusted_node1714809325384 = glueContext.create_dynamic_frame.from_catalog(database="stedi_mb", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714809325384")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714809324154 = glueContext.create_dynamic_frame.from_catalog(database="stedi_mb", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714809324154")

# Script generated for node SQL Query
SqlQuery3633 = '''
select * from acc;
'''
SQLQuery_node1714810208548 = sparkSqlQuery(glueContext, query = SqlQuery3633, mapping = {"cust":CustomerTrusted_node1714809325384, "acc":AccelerometerLanding_node1714809324154}, transformation_ctx = "SQLQuery_node1714810208548")

# Script generated for node Drop Fields
DropFields_node1714809351892 = DropFields.apply(frame=SQLQuery_node1714810208548, paths=["user", "x", "y", "z", "timestamp"], transformation_ctx="DropFields_node1714809351892")

# Script generated for node Customer Curated
CustomerCurated_node1714809359195 = glueContext.getSink(path="s3://stedi-human-balance/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1714809359195")
CustomerCurated_node1714809359195.setCatalogInfo(catalogDatabase="stedi_mb",catalogTableName="customer_curated")
CustomerCurated_node1714809359195.setFormat("json")
CustomerCurated_node1714809359195.writeFrame(DropFields_node1714809351892)
job.commit()