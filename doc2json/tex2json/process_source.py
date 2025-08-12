import argparse
import gzip
import os
from pathlib import Path
import pathlib
import tarfile
import tempfile
import time

from doc2json.tex2json.process_tex import process_tex_file, clean_tmp
from doc2json.tex2json.arxiv_to_mm import convert_to_rows, batch_to_parquet

BASE_TEMP_DIR = 'temp'
BASE_OUTPUT_DIR = 'output'
BASE_LOG_DIR = 'log'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run S2ORC SOURCE2JSON")
    parser.add_argument("-i", "--input", default="resource_tmp/debug.txt", help="path to the input TEX SOURCE file list")
    parser.add_argument("-t", "--temp", default='temp', help="path to a temp dir for partial files")
    parser.add_argument("-o", "--output", default='output', help="path to the output dir for putting json files")
    parser.add_argument("--split_size", "-s", type=int, default=200,
                        help="Split size")  # 500-1000MB 一个 parquet 文件
    parser.add_argument("-l", "--log", default='log', help="path to the log dir")
    parser.add_argument("-k", "--keep", default=True, help="keep temporary files")

    args = parser.parse_args()

    input_path = args.input
    temp_path = args.temp
    output_path = args.output
    split_size = args.split_size
    log_path = args.log
    keep_temp = args.keep

    start_time = time.time()
    
    input_path = Path(input_path)
    if input_path.suffix == '.txt':
        input_list = input_path.read_text().splitlines()
        # input_list = [line.split('/')[-1] for line in input_list if line]
    else:
        input_list = [input_path]

    failed = 0
    processed = 0
    
    for source_path in input_list:
        print(f"[INFO] start processing {source_path}")

        os.makedirs(temp_path, exist_ok=True)
        os.makedirs(output_path, exist_ok=True)  # noqa: F821
        start_time = time.time()
        
        output_file, main_tex_file = process_tex_file(source_path, temp_path, output_path, log_path, keep_temp)
        if output_file is None or main_tex_file is None:
            print(f"[ERROR] {source_path} is not a valid tex file")
            failed += 1
            continue
        
        parquet_out = output_file.replace('.json', '.parquet')
    
        batchs = convert_to_rows(Path(output_file))
        batch_to_parquet(Path(parquet_out), split_size, batchs)
        runtime = round(time.time() - start_time, 3)
        clean_tmp()
        print(f"[INFO] processed {processed} successfully")
        processed += 1


    

