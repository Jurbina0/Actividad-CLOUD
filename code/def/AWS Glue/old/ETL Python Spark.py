import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1736255733043 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://glue-inmobiliaria-bucket-1/data_landing_zone/load_date=20250102/london_houses.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1736255733043")

# Script generated for node Custom Transform
CustomTransform_node1736256024914 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1736255733043": AmazonS3_node1736255733043}, glueContext))

job.commit()