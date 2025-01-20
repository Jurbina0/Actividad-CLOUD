import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Obtener el DynamicFrame del DynamicFrameCollection
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    # Transformación: Agregar una nueva columna
    transformed_df = df.withColumn("new_column", F.lit("example_value"))
    
    # Convertir de vuelta a DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")
    
    # Retornar el DynamicFrameCollection resultante
    return DynamicFrameCollection({"transformed": transformed_dynamic_frame}, glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generado para el nodo Amazon S3
AmazonS3_node1736255733043 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://glue-inmobiliaria-bucket-1/data_landing_zone/load_date=20250102/london_houses.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1736255733043"
)

# Script generado para el nodo Custom Transform
CustomTransform_node1736256024914 = MyTransform(
    glueContext, 
    DynamicFrameCollection({"AmazonS3_node1736255733043": AmazonS3_node1736255733043}, glueContext)
)

# Validar y mostrar resultados
for frame_name, dynamic_frame in CustomTransform_node1736256024914.items():
    print(f"Resultados para {frame_name}:")
    dynamic_frame.show()

job.commit()
