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
AmazonS3_node1684043004612 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1684043004612",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="humanba",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1684043014464 = Join.apply(
    frame1=AmazonS3_node1684043004612,
    frame2=S3bucket_node1,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1684043014464",
)

# Script generated for node Drop Fields
DropFields_node1684043055178 = DropFields.apply(
    frame=Join_node1684043014464,
    paths=["user"],
    transformation_ctx="DropFields_node1684043055178",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropFields_node1684043055178,
    mappings=[
        ("timestamp", "long", "timestamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacity-project-spark-and-data-lake/data/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="humanba", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
