from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from pyspark.sql import SparkSession
import os


def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"{file_path} local file has been deleted.")
    except FileNotFoundError:
        print(f"{file_path} does not exist.")
    except PermissionError:
        print(f"Permission denied to delete {file_path}.")
    except Exception as e:
        print(f"An error occurred: {e}")


def upload_file_to_hdfs(local_file_path, hdfs_file_path):
    hdfs_put_cmd = ['hdfs', 'dfs', '-put', local_file_path, hdfs_file_path]
    print(f'STEP 3: UPLOADING ANALYSIS OUTPUT TO HDFS: {" ".join(hdfs_put_cmd)}')
    p = Popen(hdfs_put_cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    # Decode the byte output to string
    print("Output:")
    print(out.decode("utf-8"))  # Decode and print the standard output
    
    if err:
        print("Error:")
        print(err.decode("utf-8"))  # Decode and print the standard  


def upload_analysis_outputs_to_hdfs(file_name):
    # upload anaylsis output files to hdfs and clean local files
    local_files_paths = [ file_name + '_segment.tsv', file_name + '_search.tsv', file_name + '.parsed']
    hdfs_file_path = '/analysis_outputs/'
    for local_file_path in local_files_paths:
        upload_file_to_hdfs(local_file_path, hdfs_file_path)
        delete_local_file(local_file_path)

def run_parser(input_file):
    """
    Run the results_parser.py over the hhr file to produce the output summary
    """
    search_file = input_file+"_search.tsv"
    print("search_file: ", search_file)
    cmd = ['python3', './results_parser.py', search_file]
    print(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    # Decode the byte output to string
    print("Output:")
    print(out.decode("utf-8"))  # Decode and print the standard output
        
    if err:
        print("Error:")
        print(err.decode("utf-8"))  # Decode and print the standard error

def run_merizo_search(file_name, file_content):
    print(f"File Name: {file_name}")
    # Create a temporary file to hold the content
    with NamedTemporaryFile(delete=True, mode='wb') as temp_file:
        temp_file.write(file_content)
        temp_file_path = temp_file.name
        cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           temp_file_path,
           '/home/almalinux/data/cath-4.3-foldclassdb',
           file_name,
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
        print(out.decode("utf-8"))  # Decode and print the standard output
        
        if err:
            print("Error:")
            print(err.decode("utf-8"))  # Decode and print the standard 


def pipeline(file_tuple):
    file_name, file_content = file_tuple
    # STEP 1
    run_merizo_search(file_name, file_content)
    # STEP 2
    run_parser(file_name)
    # STEP 3
    upload_analysis_outputs_to_hdfs(file_name)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MerizoSearch").getOrCreate()
    sc = spark.sparkContext

    input_dir = "/UP000000625_83333_ECOLI_v4/"
    file_rdd = sc.binaryFiles(input_dir + "*.pdb")
    # file_rdd = file_rdd.sample(withReplacement=False, fraction=0.005)
    file_content_rdd = file_rdd.map(lambda x: (os.path.basename(x[0]), x[1]))

    file_content_rdd.map(pipeline).collect()
