from pyspark.sql import SparkSession
from subprocess import Popen, PIPE
import sys
import os

"""
usage: python pipeline_script.py [INPUT DIR] [OUTPUT DIR]
"""

def run_parser(input_file, output_dir):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    search_file = input_file + "_search.tsv"
    print(search_file, output_dir)
    cmd = ['python', './results_parser.py', output_dir, search_file]
    print(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print(out.decode("utf-8"))

def run_merizo_search(input_file, id):
    """
    Runs the merizo domain predictor to produce domains
    """
    cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           input_file,
           '/home/almalinux/merizo_search/examples/database/cath-4.3-foldclassdb',
           id,
           'tmp',
           '--iterate',
           '--output_headers',
           '-d',
           'cpu',
           '--threads',
           '1'
           ]
    print(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()

def read_dir(input_dir, output_dir):
    """
    Read file paths from HDFS using SparkContext.
    """
    # Use SparkContext to read files from HDFS
    # If files are located in a directory on HDFS, you can use textFile 
    file_rdd = sc.textFile(input_dir + "/AF-V9HVX0-F1-model_v4.pdb") 
    file_paths = file_rdd.collect()  # This retrieves the file paths as a list

    # Create a list of tuples with file path, id, and output directory
    return [(file_path, os.path.basename(file_path), output_dir) for file_path in file_paths]

def pipeline(file_info):
    filepath, id, outpath = file_info
    run_merizo_search(filepath, id)
    run_parser(id, outpath)

if __name__ == "__main__":

    print(f'/////////////////////////////////////////////////// RUNNING SCRIPT //////////////////////////////////////////////')

    input_dir = sys.argv[1]  # HDFS directory path (hdfs://namenode_host:8020/path/to/files)
    output_dir = sys.argv[2]

    # Initialize Spark
    spark = SparkSession.builder.appName("MerizoPipeline").getOrCreate()
    sc = spark.sparkContext

    # Read files from HDFS and create an RDD
    pdbfiles = read_dir(input_dir, output_dir)
    pdb_rdd = sc.parallelize(pdbfiles)

    # Run the pipeline in parallel using RDD operations
    pdb_rdd.map(pipeline)

