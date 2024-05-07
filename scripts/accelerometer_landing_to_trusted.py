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

# Script generated for node accelerometer landing
accelerometerlanding_node1714667417346 = glueContext.create_dynamic_frame.from_catalog(database="stedi_mb", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1714667417346")

# Script generated for node customer Trusted
customerTrusted_node1714807888587 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/customer/trusted2/"], "recurse": True}, transformation_ctx="customerTrusted_node1714807888587")

# Script generated for node Inner Join
SqlQuery302 = '''
select acc.* from acc join cust on acc.user=cust.email;
'''
InnerJoin_node1714807394650 = sparkSqlQuery(glueContext, query = SqlQuery302, mapping = {"acc":accelerometerlanding_node1714667417346, "cust":customerTrusted_node1714807888587}, transformation_ctx = "InnerJoin_node1714807394650")

# Script generated for node accel trusted
acceltrusted_node1714668378622 = glueContext.getSink(path="s3://stedi-human-balance/accelerometer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="acceltrusted_node1714668378622")
acceltrusted_node1714668378622.setCatalogInfo(catalogDatabase="stedi_mb",catalogTableName="accelerometer_trusted2")
acceltrusted_node1714668378622.setFormat("json")
acceltrusted_node1714668378622.writeFrame(InnerJoin_node1714807394650)
job.commit()