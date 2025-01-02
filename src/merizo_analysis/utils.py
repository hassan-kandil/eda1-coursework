import os
import shutil
from subprocess import Popen, PIPE
import tarfile
from merizo_analysis.config import logger


def run_command(cmd: str):
    command_string = " ".join(cmd)
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    # Decode the byte output to string
    logger.info(f"Command {command_string} Output: \n{out.decode('utf-8')}")

    if err:
        logger.error(f"Command {command_string} Error: \n{err.decode('utf-8')}")


def compress_directory(directory_path, tar_gz_path):
    # Compress the directory into a tar.gz file
    logger.info(f"Compressing directory {directory_path} into {tar_gz_path}")
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        tar.add(directory_path, arcname=".")
    logger.info(f"Directory {directory_path} compressed into {tar_gz_path}")

def delete_local_directory(directory_path):
    try:
        shutil.rmtree(directory_path)
        logger.info(f"{directory_path} local directory has been deleted.")
    except FileNotFoundError:
        logger.error(f"{directory_path} does not exist.")
        raise
    except PermissionError:
        logger.error(f"Permission denied to delete {directory_path}.")
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

def delete_local_file(file_path):
    try:
        os.remove(file_path)
        logger.info(f"{file_path} local file has been deleted.")
    except FileNotFoundError:
        logger.error(f"{file_path} does not exist.")
        raise
    except PermissionError:
        logger.error(f"Permission denied to delete {file_path}.")
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
