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
AmazonS3_node1684038316948 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1684038316948",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba", table_name="accelerometer", transformation_ctx="S3bucket_node1"
)

# Script generated for node Join
Join_node1684038349649 = Join.apply(
    frame1=AmazonS3_node1684038316948,
    frame2=S3bucket_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1684038349649",
)

# Script generated for node Drop Fields
DropFields_node1684038382474 = DropFields.apply(
    frame=Join_node1684038349649,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1684038382474",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropFields_node1684038382474,
    mappings=[
        ("registrationdate", "long", "registrationdate", "long"),
        ("customername", "string", "customername", "string"),
        ("birthday", "string", "birthday", "string"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("email", "string", "email", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("phone", "string", "phone", "string"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacity-project-spark-and-data-lake/data/customers_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="humanba", catalogTableName="customers_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
