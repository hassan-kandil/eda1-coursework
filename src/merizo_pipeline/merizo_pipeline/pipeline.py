import sys
from subprocess import Popen, PIPE
import glob
import os
import multiprocessing
from merizo_pipeline.parser import parse_results 

def run_merizo_search(input_file, id):
    cmd = [
        'python3',
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
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()

def read_dir(input_dir):
    print("Getting file list")
    file_ids = list(glob.glob(input_dir + "*.pdb"))
    analysis_files = []
    for file in file_ids:
        id = file.rsplit('/', 1)[-1]
        analysis_files.append([file, id])
    return analysis_files

def pipeline(filepath, id, outpath):
    run_merizo_search(filepath, id)
    parse_results(id, outpath)

def main():
    pdbfiles = read_dir(sys.argv[1])
    p = multiprocessing.Pool(1)
    p.starmap(pipeline, [(f[0], f[1], sys.argv[2]) for f in pdbfiles[:10]])
