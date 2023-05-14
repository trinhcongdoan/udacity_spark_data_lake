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

# Script generated for node Amazon S3
AmazonS3_node1684033786035 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1684033786035",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba", table_name="accelerometer", transformation_ctx="S3bucket_node1"
)

# Script generated for node Join
Join_node1684033801774 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1684033786035,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684033801774",
)

# Script generated for node Drop Fields
DropFields_node1684033876674 = DropFields.apply(
    frame=Join_node1684033801774,
    paths=[
        "registrationdate",
        "customername",
        "birthday",
        "sharewithpublicasofdate",
        "lastupdatedate",
        "email",
        "serialnumber",
        "phone",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1684033876674",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropFields_node1684033876674,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "long", "timestamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacity-project-spark-and-data-lake/data/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="humanba", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
