from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from pyspark.sql import SparkSession
import os

def run_merizo_search(file_tuple):
    input_file_name, file_content = file_tuple
    print(f"File Name: {input_file_name}")
    # Create a temporary file to hold the content
    with NamedTemporaryFile(delete=True, mode='wb') as temp_file:
        temp_file.write(file_content)
        temp_file_path = temp_file.name
    
    cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           temp_file_path,
           '/home/almalinux/data/cath-4.3-foldclassdb',
           input_file_name,
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
    # Decode the byte output to string
    print("Output:")
    print(out.decode())  # Decode and print the standard output
    
    if err:
        print("Error:")
        print(err.decode())  # Decode and print the standard error

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MerizoSearch").getOrCreate()
    sc = spark.sparkContext

    input_dir = "/UP000000625_83333_ECOLI_v4/"
    file_rdd = sc.binaryFiles(input_dir + "*.pdb")
    file_content_rdd = file_rdd.map(lambda x: (os.path.basename(x[0]), x[1]))

    file_content_rdd.map(run_merizo_search).collect()
