from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile

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
    hdfs_put_cmd = ['/home/almalinux/hadoop-3.4.0/bin/hdfs', 'dfs', '-put', local_file_path, hdfs_file_path]
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
    cmd = ['python3', '/home/almalinux/eda1-coursework/src/merizo_pipeline/results_parser.py', search_file]
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
    # Setting matplotlib config env var to a writable tmp directory 
    # since it's a merizo search dependency
    os.environ['MPLCONFIGDIR'] = '/tmp/matplotlib_config'
    # Create a temporary file to hold the pdb content 
    # since merizo search requires the input to be a physical file
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
           '2'
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

