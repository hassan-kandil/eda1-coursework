from subprocess import PIPE, Popen
from pyspark.sql import SparkSession
import os
import logging
import time

from merizo_analysis.pipeline import pipeline
from merizo_analysis.combine import extract_results, zero_value, seq_op, comb_op
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_df_to_hdfs_csv(df, hdfs_path, csv_file_name):
    print(f'WRITING ANALYSIS SUMMARY OUTPUT {csv_file_name} TO HDFS...')
    write_path = hdfs_path + csv_file_name
    df.write.option("header","true").mode("overwrite").csv(write_path)
    hdfs_mv_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-mv', write_path + '/part-00000-*.csv', write_path + '.csv']
    p = Popen(hdfs_mv_cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    # Decode the byte output to string
    print("Output:")
    print(out.decode("utf-8"))  # Decode and print the standard output
    
    if err:
        print("Error:")
        print(err.decode("utf-8"))  # Decode and print the standard  

def prepare_rdd(sc, input_dir):
    file_rdd = sc.binaryFiles(input_dir + "*.pdb")
    file_rdd = file_rdd.sample(withReplacement=False, fraction=0.001)
    file_content_rdd = file_rdd.map(lambda x: (os.path.basename(x[0]), x[1]))
    return file_content_rdd

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MerizoPipeline") \
        .getOrCreate()
    sc = spark.sparkContext

    ecoli_dir = "/UP000000625_83333_ECOLI_v4/"
    human_dir = "/UP000005640_9606_HUMAN_v4/"

    ecoli_content_rdd = prepare_rdd(sc, ecoli_dir)
    human_content_rdd = prepare_rdd(sc, human_dir)

    # Use aggregate to compute the result
    start_time = time.time()
    ecoli_result = ecoli_content_rdd.map(pipeline).aggregate(zero_value, seq_op, comb_op)
    human_result = human_content_rdd.map(pipeline).aggregate(zero_value, seq_op, comb_op)

    # Extract results
    human_mean, human_population_std_dev, human_combined_dict = extract_results(human_result)
    ecoli_mean, ecoli_population_std_dev, ecoli_combined_dict = extract_results(ecoli_result)

    end_time = time.time()
    print(f"Done with the pipeline in {end_time - start_time:.2f} seconds")  

    # Convert results to DataFrames
    stats_df = spark.createDataFrame(
        [("human", human_mean, human_population_std_dev), ("ecoli", ecoli_mean, ecoli_population_std_dev)],
        schema=StructType([
            StructField("organism", StringType(), True),
            StructField("mean plddt", FloatType(), True),
            StructField("plddt std", FloatType(), True),
        ])
    ).coalesce(1)

    stats_df.show()

    ecoli_dict_df = spark.createDataFrame(
        [(key, value) for key, value in ecoli_combined_dict.items()],
        schema=StructType([
            StructField("cath_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ])
    ).coalesce(1)

    ecoli_dict_df.show()

    human_dict_df = spark.createDataFrame(
        [(key, value) for key, value in human_combined_dict.items()],
        schema=StructType([
            StructField("cath_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ])
    ).coalesce(1)

    human_dict_df.show()

    write_df_to_hdfs_csv(stats_df, "/summary_outputs/", "pIDDT_means")
    write_df_to_hdfs_csv(ecoli_dict_df, "/summary_outputs/", "ecoli_cath_summary")
    write_df_to_hdfs_csv(human_dict_df, "/summary_outputs/", "human_cath_summary")


