from pyspark.sql import SparkSession
import os
import logging

from merizo_pipeline.pipeline import pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MerizoPipeline") \
        .getOrCreate()
    sc = spark.sparkContext

    input_dir = "/UP000000625_83333_ECOLI_v4/"
    file_rdd = sc.binaryFiles(input_dir + "*.pdb")
    # file_rdd = file_rdd.sample(withReplacement=False, fraction=0.001)
    file_content_rdd = file_rdd.map(lambda x: (os.path.basename(x[0]), x[1]))

    file_content_rdd.map(pipeline).collect()
