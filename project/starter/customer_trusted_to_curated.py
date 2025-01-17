import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://darkstar-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1689902219430 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://darkstar-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1689902219430",
)

# Script generated for node Join Customer
JoinCustomer_node1689902319411 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerTrusted_node1689902219430,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1689902319411",
)

# Script generated for node Drop Fields
DropFields_node1689902396080 = DropFields.apply(
    frame=JoinCustomer_node1689902319411,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1689902396080",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689902396080,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://darkstar-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
