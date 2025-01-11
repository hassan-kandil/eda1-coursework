from subprocess import CalledProcessError, Popen, PIPE
from tempfile import NamedTemporaryFile

import os

from merizo_analysis.utils import delete_local_file, run_command
from merizo_analysis.config import logger


def upload_files_to_hdfs(local_files_paths, hdfs_file_path):
    hdfs_put_cmd = [
        "/home/almalinux/hadoop-3.4.0/bin/hdfs",
        "dfs",
        "-put",
        *local_files_paths,
        hdfs_file_path,
    ]
    logger.info(f'STEP 3: UPLOADING ANALYSIS OUTPUT TO HDFS: {" ".join(hdfs_put_cmd)}')
    run_command(hdfs_put_cmd)


def upload_analysis_outputs_to_hdfs(file_name):
    # upload anaylsis output files to hdfs and clean local files
    local_files_paths = [
        file_name + "_segment.tsv",
        file_name + "_search.tsv",
        file_name + ".parsed",
    ]
    hdfs_file_path = "/analysis_outputs/"
    files_to_upload = [
        local_file for local_file in local_files_paths if os.path.exists(local_file)
    ]
    upload_files_to_hdfs(files_to_upload, hdfs_file_path)
    for uploaded_file in files_to_upload:
        delete_local_file(uploaded_file)


def run_parser(input_file):
    """
    Run the results_parser.py over the tsv search file to produce the output summary
    """
    search_file = input_file + "_search.tsv"
    if not os.path.exists(search_file):
        logger.info(f"Search file {search_file} does not exist. Skipping the parser.")
        return
    cmd = [
        "python3",
        "/home/almalinux/merizo_pipeline/merizo_analysis/results_parser.py",
        search_file,
    ]
    logger.info(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    run_command(cmd)


def run_merizo_search(file_name, file_content):
    logger.info(f"File Name: {file_name}")
    # Setting matplotlib config env var to a writable tmp directory
    # since it's a merizo search dependency
    os.environ["MPLCONFIGDIR"] = "/tmp/matplotlib_config"
    # Create a temporary file to hold the pdb content
    # since merizo search requires the input to be a physical file
    with NamedTemporaryFile(delete=True, mode="w") as temp_file:
        temp_file.write(file_content)
        temp_file_path = temp_file.name
        cmd = [
            "python3",
            "/home/almalinux/merizo_search/merizo_search/merizo.py",
            "easy-search",
            temp_file_path,
            "/home/almalinux/data/cath-4.3-foldclassdb",
            file_name,
            "tmp",
            "--iterate",
            "--output_headers",
            "-d",
            "cpu",
            "--threads",
            "1",
        ]
        logger.info(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
        p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        _, err = p.communicate()
        logger.info(
            f"Command {''.join(cmd)} Output: \n{err.decode('utf-8')}"
        )  # Using stderr since merizo writes everything to stderr
        if p.returncode != 0:
            logger.error(f"Command {''.join(cmd)} Error: \n{err.decode('utf-8')}")


def read_parsed_file(file_name):
    """
    Reads a .parsed file and extracts:
    1. A dictionary with 'cath_id' as keys and their counts as values.
    2. The mean pLDDT value.

    Args:
        file_path (str): Path to the .parsed file.

    Returns:
        tuple: (dict of cath_id counts, mean pLDDT value)
    """
    file_path = file_name + ".parsed"
    if not os.path.exists(file_path):
        return None, None  # Skipping unsegmentable pdb files

    cath_counts = {}
    mean_plddt = 0.0

    with open(file_path, "r") as file:
        lines = file.readlines()

        # Extract mean pLDDT from the header
        if len(lines) < 1:
            return cath_counts, mean_plddt

        first_line = lines[0]
        if "mean plddt:" in first_line:
            mean_plddt = float(first_line.split("mean plddt:")[1].strip())

        # Skip the header and process the data rows
        for line in lines[2:]:  # Assuming data rows start from the 3rd line
            if not line.strip():
                continue  # Ignore empty lines
            cath_id, count = line.strip().split(",")
            cath_counts[cath_id] = int(count)

    return mean_plddt, cath_counts


def pipeline(file_tuple):
    file_name, file_content = file_tuple
    # STEP 1
    run_merizo_search(file_name, file_content)
    # STEP 2
    run_parser(file_name)
    # STEP 3
    mean_plddt, cath_counts_dict = read_parsed_file(file_name)
    # STEP 4
    upload_analysis_outputs_to_hdfs(file_name)
    return mean_plddt, cath_counts_dict
