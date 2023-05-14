import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1684027402781 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-spark-and-data-lake/data/customers/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684027402781",
)

# Script generated for node Filter
Filter_node1684027445179 = Filter.apply(
    frame=AmazonS3_node1684027402781,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1684027445179",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1684029224267 = glueContext.write_dynamic_frame.from_catalog(
    frame=Filter_node1684027445179,
    database="humanba",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1684029224267",
)

job.commit()
