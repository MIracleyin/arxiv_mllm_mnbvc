import argparse
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import pandas as pd
import os
# from daily_tools import load_jsonl, save_jsonl
import glob
import sys


def save_jsonl(content, file_path, new=False, print_log=True):
    if print_log:
        # print(f"save to {file_path}")
        print(content)
    try:
        with open(file_path, "a+" if not new else "w") as outfile:
            # for entry in content:
            # json.dump(entry, outfile, ensure_ascii=False)
            outfile.write(json.dumps(content, ensure_ascii=False))  # noqa: F821
            outfile.write("\n")
    except Exception as e:
        print(e)
        print("Error: error when saving jsonl!!!!")


def load_jsonl(data_path):
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        jsonl_list = []
        for line in lines:
            try:
                jsonl_list.append(json.loads(line))
            except:
                print(f"{line} is not a valid json string")
        # jsonl_list = [json.loads(line) for line in lines]
    else:
        print(f"{data_path} not exists!")
        jsonl_list = []
    return jsonl_list

def concat_data(paramters):
    try:
        file_paths, global_log_file, save_path = paramters

        print("global_log_file", global_log_file)
        print("save_path", save_path)

        total_parquet = []
        for each_path in file_paths:
            try:
                df = pd.read_parquet(each_path)
                total_parquet.append(df)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                # 保存异常信息到日志文件中
                save_jsonl(
                    {
                        "reason": {
                            "e": str(e),
                            "exc_type": str(exc_type),
                            "exc_value": str(exc_value),
                            "exc_traceback": repr(exc_traceback),
                        },
                        "file": each_path,
                        "save_path": save_path,
                    },
                    global_log_file,
                )

        df_all = pd.concat(total_parquet)
        df_all.to_parquet(save_path, index=False)
    except Exception as e:
        print(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # 保存异常信息到日志文件中
        save_jsonl(
            {
                "reason": {
                    "e": str(e),
                    "exc_type": str(exc_type),
                    "exc_value": str(exc_value),
                    "exc_traceback": str(exc_traceback),
                },
                "save_path": save_path,
            },
            global_log_file,
        )


def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument("--input_dir", "-i", type=str, required=True, help="Input file")
    parser.add_argument("--output_dir", "-o", type=str, required=True, help="Output directory")
    parser.add_argument("--log_dir", "-l", type=str, default="logs", help="Log path")
    parser.add_argument("--num_of_file", "-n", type=int, default=10000, help="concat num")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d")

    input_file = args.input_dir
    # log_dir = Path(args.log_dir)

    # global global_log_file, output_dir # 失败日志
    os.makedirs(args.log_dir, exist_ok=True)
    global_log_file = os.path.join(args.log_dir, "log_file_while_concat.log")
    output_dir = args.output_dir

    Path(global_log_file).touch()  # 创建全局日志文件
    os.makedirs(output_dir, exist_ok=True)

    # txt 文件为在数据路径下生成的 list 文件
    # ex: find . -name "*.pdf" > list.txt
    if os.path.exists(input_file):
        input_file_path_list = glob.glob(os.path.join(input_file, "**", "*.parquet"), recursive=True)
        print(input_file_path_list)
        # with ProcessPoolExecutor(max_workers=workers_num) as executor:
        #     process_file = []
        #     for start in range(0, len(input_file_path_list), args.num_of_file):
        #         save_path = os.path.join(
        #             output_dir, f"{start}_{start + args.num_of_file}.parquet"
        #         )
        #         process_file.append(
        #             [
        #                 input_file_path_list[start : start + args.num_of_file],
        #                 global_log_file,
        #                 save_path,
        #             ]
        #         )

        #     executor.map(
        #         concat_data,
        #         process_file,
        #     )

        # with ProcessPoolExecutor(max_workers=workers_num) as executor:
        for start in range(0, len(input_file_path_list), args.num_of_file):
            save_path = os.path.join(
                output_dir, f"{start}_{start + args.num_of_file}.parquet"
            )
            concat_data([
                    input_file_path_list[start : start + args.num_of_file],
                    global_log_file,
                    save_path,
                ])
    else:
        assert False, "Input file must be a text file"


if __name__ == "__main__":
    main()
