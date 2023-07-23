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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://darkstar-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1690072067751 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://darkstar-lake-house/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedZone_node1690072067751",
    )
)

# Script generated for node Join
Join_node1690072112195 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrustedZone_node1690072067751,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1690072112195",
)

# Script generated for node Step Trainer Curated
StepTrainerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690072112195,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://darkstar-lake-house/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerCurated_node3",
)

job.commit()
