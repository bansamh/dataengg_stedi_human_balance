import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1714388887799 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1714388887799")

# Script generated for node Share with Research
SharewithResearch_node1714388998973 = Filter.apply(frame=CustomerLanding_node1714388887799, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="SharewithResearch_node1714388998973")

# Script generated for node Customer Trusted
CustomerTrusted_node1714389153503 = glueContext.getSink(path="s3://stedi-human-balance/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1714389153503")
CustomerTrusted_node1714389153503.setCatalogInfo(catalogDatabase="stedi_mb",catalogTableName="customer_trusted")
CustomerTrusted_node1714389153503.setFormat("json")
CustomerTrusted_node1714389153503.writeFrame(SharewithResearch_node1714388998973)
job.commit()