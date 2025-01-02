from pyspark.sql import SparkSession
import os
import logging
import time

from merizo_analysis.pipeline import pipeline
from merizo_analysis.utils import compress_directory, delete_local_directory, delete_local_file, run_command
from merizo_analysis.combine import extract_results, zero_value, seq_op, comb_op
from merizo_analysis.config import logger
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType


def compress_hdfs_output_dir(hdfs_path):
    logger.info(f"Compressing HDFS output directory: {hdfs_path}")
    dir_name = os.path.basename(os.path.normpath(hdfs_path))
    tar_file_name = f"{dir_name}.tar.gz"
    hdfs_get_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-get', hdfs_path]
    run_command(hdfs_get_cmd)
    compress_directory(dir_name, tar_file_name)
    hdfs_put_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-put', tar_file_name, "/"]
    run_command(hdfs_put_cmd)
    delete_local_directory(dir_name)
    delete_local_file(tar_file_name)

def write_df_to_hdfs_csv(df, hdfs_path, csv_file_name):
    logger.info(f'WRITING ANALYSIS SUMMARY OUTPUT {csv_file_name} TO HDFS...')
    write_path = hdfs_path + csv_file_name
    df.write.option("header","true").mode("overwrite").csv(write_path)
    hdfs_mv_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-mv', write_path + '/part-00000-*.csv', write_path + '.csv']
    run_command(hdfs_mv_cmd)
    hdfs_rm_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-rm', '-r', write_path]
    run_command(hdfs_rm_cmd)

def create_df_from_dict(cath_count_dict):
    cath_count_df = spark.createDataFrame(
        [(key, value) for key, value in cath_count_dict.items()],
        schema=StructType([
            StructField("cath_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ])
    ).coalesce(1)
    return cath_count_df

def prepare_rdd(sc, input_dir):
    # Failed example : AF-P0DSE5-F1-model_v4.pdb
    # Success example: AF-P75975-F1-model_v4.pdb with empty parsed file
    # Success example: AF-P67430-F1-model_v4.pdb with non-empty parsed file
    logger.info(f"Reading files from {input_dir}")
    # files = ["AF-P0DSE5-F1-model_v4.pdb", "AF-P75975-F1-model_v4.pdb", "AF-P67430-F1-model_v4.pdb"]
    # file_paths = []
    # for file in files:
    #     file_path = input_dir + file
    #     file_paths.append(file_path)
    file_rdd = sc.binaryFiles(input_dir + "*.pdb")
    # file_rdd = sc.binaryFiles(','.join(file_paths))
    # file_rdd = file_rdd.sample(withReplacement=False, fraction=0.001)
    file_content_rdd = file_rdd.map(lambda x: (os.path.basename(x[0]), x[1]))
    return file_content_rdd

def run_full_pipeline(spark, dataset_name, input_dir):
    sc = spark.sparkContext
    content_rdd = prepare_rdd(sc, input_dir)
    result = content_rdd.map(pipeline).aggregate(zero_value, seq_op, comb_op)
    mean, population_std_dev, combined_dict = extract_results(result)

    dict_df = create_df_from_dict(combined_dict)
    stats_df = spark.createDataFrame(
        [(dataset_name, mean, population_std_dev)],
        schema=StructType([
            StructField("organism", StringType(), True),
            StructField("mean plddt", FloatType(), True),
            StructField("plddt std", FloatType(), True),
        ])
    ).coalesce(1)

    write_df_to_hdfs_csv(stats_df, "/summary_outputs/", "pIDDT_means")
    write_df_to_hdfs_csv(dict_df, "/summary_outputs/", f"{dataset_name}_cath_summary")
    compress_hdfs_output_dir("/summary_outputs/")
    compress_hdfs_output_dir("/analysis_outputs/")

    return stats_df, dict_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MerizoPipeline") \
        .getOrCreate()

    ecoli_dir = "/UP000000625_83333_ECOLI_v4/"
    human_dir = "/UP000005640_9606_HUMAN_v4/"

    try:
        start_time = time.time()
        logger.info("Running pipeline job..")
        ecoli_stats, ecoli_cath_count = run_full_pipeline(spark, "ecoli", ecoli_dir)
        human_stats, human_cath_count = run_full_pipeline(spark, "human", human_dir)
        logger.info(f"Pipeline job finished in {time.time() - start_time:.2f} seconds.")
    except Exception as e:
        logger.exception(f"Error running pipeline: {e}")
        raise e
    finally:
        logger.info("Stopping Spark session..")
        spark.stop()

