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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://darkstar-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1689905454281 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://darkstar-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1689905454281",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1689908654033 = ApplyMapping.apply(
    frame=CustomerCurated_node1689905454281,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "right_shareWithPublicAsOfDate",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "right_shareWithResearchAsOfDate",
            "bigint",
        ),
        ("registrationDate", "bigint", "right_registrationDate", "bigint"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"),
        ("phone", "string", "right_phone", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "right_shareWithFriendsAsOfDate",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1689908654033",
)

# Script generated for node Join
Join_node1689905645008 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1689908654033,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1689905645008",
)

# Script generated for node Drop Fields
DropFields_node1689906349272 = DropFields.apply(
    frame=Join_node1689905645008,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_shareWithPublicAsOfDate",
        "right_shareWithResearchAsOfDate",
        "right_customerName",
        "right_registrationDate",
        "right_email",
        "right_lastUpdateDate",
        "right_phone",
        "right_shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689906349272",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689906349272,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://darkstar-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedZone_node3",
)

job.commit()
