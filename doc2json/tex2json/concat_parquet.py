import argparse
from pathlib import Path
import os
# from daily_tools import load_jsonl, save_jsonl
import glob
import sys
import json
from typing import List, Tuple

import pyarrow.parquet as pq


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
            except Exception:
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

        # 使用 Arrow 以行组为单位流式写入，避免一次性加载到内存
        writer = None
        try:
            # 首先读取第一个文件来获取基础schema
            first_file = file_paths[0]
            first_pf = pq.ParquetFile(first_file)
            base_schema = first_pf.schema_arrow
            
            # 创建统一的schema，移除pandas metadata以避免不匹配
            unified_schema = base_schema.remove_metadata()
            
            # 创建writer使用统一的schema
            writer = pq.ParquetWriter(save_path, unified_schema, compression="snappy")
            
            # 处理所有文件
            for each_path in file_paths:
                try:
                    pf = pq.ParquetFile(each_path)
                    for rg_idx in range(pf.num_row_groups):
                        table = pf.read_row_group(rg_idx)
                        # 确保table的schema与writer的schema兼容
                        if table.schema != unified_schema:
                            # 如果schema不匹配，尝试转换
                            try:
                                # 使用pyarrow的cast功能来确保schema兼容
                                table = table.cast(unified_schema)
                            except Exception as cast_error:
                                # 如果cast失败，记录错误但继续处理
                                save_jsonl(
                                    {
                                        "reason": {
                                            "e": f"Schema cast failed: {str(cast_error)}",
                                            "exc_type": "SchemaCastError",
                                            "exc_value": str(cast_error),
                                            "exc_traceback": repr(cast_error),
                                        },
                                        "file": each_path,
                                        "save_path": save_path,
                                    },
                                    global_log_file,
                                )
                                continue
                        writer.write_table(table)
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
        finally:
            if writer is not None:
                writer.close()
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
    parser.add_argument("--num_of_file", "-n", type=int, default=10000, help="concat num (deprecated when using size-based sharding)")
    parser.add_argument("--target_size_gb", type=float, default=5.0, help="Target shard size in GB (approximate, based on input parquet file sizes)")
    args = parser.parse_args()

    # current_date = datetime.now().strftime("%Y-%m-%d")  # unused

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
        print(f"found parquet files: {len(input_file_path_list)}")

        # 基于输入文件的压缩大小进行 5GB（可配置）左右的分桶，尽量让每个桶的合计大小接近目标
        target_bytes = int(args.target_size_gb * (1024 ** 3))

        files_with_size: List[Tuple[str, int]] = []
        for p in input_file_path_list:
            try:
                sz = os.path.getsize(p)
            except Exception:
                sz = 0
            files_with_size.append((p, sz))

        # 先按文件大小降序排序，再做 First-Fit-Decreasing 装箱，尽量填满每个桶到目标上限
        files_with_size.sort(key=lambda x: x[1], reverse=True)

        bins: List[List[str]] = []
        bin_sizes: List[int] = []

        for p, sz in files_with_size:
            placed = False
            for i in range(len(bins)):
                if bin_sizes[i] + sz <= target_bytes:
                    bins[i].append(p)
                    bin_sizes[i] += sz
                    placed = True
                    break
            if not placed:
                bins.append([p])
                bin_sizes.append(sz)

        print(f"planned {len(bins)} shards with target ~{args.target_size_gb}GB each")

        # 顺序写出每个 shard
        for shard_idx, file_list in enumerate(bins):
            save_path = os.path.join(output_dir, f"shard_{shard_idx:05d}.parquet")
            print(f"writing {save_path} with {len(file_list)} files, planned size ~{bin_sizes[shard_idx]/(1024**3):.2f}GB")
            concat_data([
                file_list,
                global_log_file,
                save_path,
            ])
    else:
        assert False, "Input file must be a text file"


if __name__ == "__main__":
    main()
