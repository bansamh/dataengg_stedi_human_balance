import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714393017393 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1714393017393")

# Script generated for node Customer Trusted
CustomerTrusted_node1714393014664 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1714393014664")

# Script generated for node Join
Join_node1714393034915 = Join.apply(frame1=AccelerometerLanding_node1714393017393, frame2=CustomerTrusted_node1714393014664, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1714393034915")

# Script generated for node Drop Fields
DropFields_node1714395518445 = DropFields.apply(frame=Join_node1714393034915, paths=["serialnumber", "sharewithpublicasofdate", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "sharewithfriendsasofdate", "email", "lastupdatedate", "phone"], transformation_ctx="DropFields_node1714395518445")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714393213677 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1714395518445, connection_type="s3", format="json", connection_options={"path": "s3://stedi-human-balance/accelerometer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AccelerometerTrusted_node1714393213677")

job.commit()