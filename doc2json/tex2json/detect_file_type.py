import json
import os
import magic
import shutil
import tarfile
import gzip
import argparse
import glob
# from daily_tools import save_jsonl, load_jsonl



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

def detect_file_type(file_path):
    # 使用magic库检测文件类型
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    
    # 根据MIME类型返回文件扩展名
    if file_type == 'application/x-gzip' or file_type == 'application/gzip':
        # 进一步检查是否是 tar.gz 文件
        try:
            with gzip.open(file_path, 'rb') as f:
                # 尝试读取 tar 文件头
                tarfile.open(fileobj=f)
            return '.tar.gz'
        except (tarfile.TarError, gzip.BadGzipFile, OSError):
            return '.gz'
    elif file_type == 'application/x-tar':
        return '.tar'
    elif file_type == 'application/zip':
        return '.zip'
    elif file_type == 'application/pdf':  # 添加PDF类型检测
        return '.pdf'
    elif file_type == 'text/plain':
        return '.txt'
    elif file_type == 'text/x-tex':
        return '.tex'
    else:
        return ""


def save_with_append(file_path, content):
    with open(file_path, 'a+', encoding='utf-8') as f:
        f.write(content)

def auto_remove_file(file_path):
    # for file_path in files:
    if os.path.exists(file_path):
        os.remove(file_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, required=True, help="Directory containing files to be renamed.")
    parser.add_argument("--image2caption_processed_file", type=str, 
                        default="./resource_tmp/image2caption_processed_file.txt", 
                        help="Save the list of renamed files to a text file.")
    parser.add_argument("--tableequation_processed_file", type=str, 
                        default="./resource_tmp/tableequation_processed_file.txt", 
                        help="Save the list of renamed files to a text file.")
    parser.add_argument("--unprocessed_text", type=str, default="./resource_tmp/unprocessed_text_file_list.txt", 
                        help="Save the list of unprocessed files to a text file.")
    parser.add_argument("--failed_text", type=str, default="./resource_tmp/failed_text_file_list.txt", help="Save the list of failed files to a text file.")
    parser.add_argument("--source_text_file", type=str, default="./resource_tmp/source_text_file_list.txt", help="Save the list of source tex files to a text file.")
    parser.add_argument("--new_file", action="store_false", help="Create a new file!")
    args = parser.parse_args()
    os.makedirs(os.path.dirname(args.image2caption_processed_file), exist_ok=True)
    os.makedirs(os.path.dirname(args.tableequation_processed_file), exist_ok=True)
    os.makedirs(os.path.dirname(args.unprocessed_text), exist_ok=True)
    os.makedirs(os.path.dirname(args.failed_text), exist_ok=True)
    if args.new_file:
        auto_remove_file(args.image2caption_processed_file)
        auto_remove_file(args.tableequation_processed_file)
        auto_remove_file(args.unprocessed_text)
        auto_remove_file(args.failed_text)

    for file_path in glob.glob(os.path.join(args.dir, "**", "source", "*"), recursive=True):
        if os.path.isfile(file_path):
            try:
                extension = detect_file_type(file_path)
                if extension:
                    new_file_path = file_path + extension

                    new_file_path = new_file_path.replace('/source/', '/source_extentions/')
                    os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
                    # rename_file_with_extension(file_path, extension)
                    # 重命名文件
                    shutil.copyfile(file_path, new_file_path) # 注意这里是备份了一份源文件，但是如果不需要则可以改为重命名源文件
                    print(f"Copy {file_path} to {new_file_path}")
                    if extension in ['.tar.gz', '.tar', '.gz', '.zip', '.tex']:
                        save_with_append(args.source_text_file, new_file_path+'\n')
                    # if extension not in [".pdf", ".txt", ".tex"]:
                    #     save_with_append(args.image2caption_processed_file, new_file_path+'\n')

                    # if extension not in [".pdf"]:
                    #     save_with_append(args.tableequation_processed_file, new_file_path+'\n')
                    # else:
                    #     save_with_append(args.unprocessed_text, new_file_path+'\n')
                else:
                    print(f"Cannot determine the type of {file_path}")
                    save_jsonl( [{"file_path": file_path, "error": "Unknown file type"}], args.failed_text)
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")
                save_jsonl([{"file_path": file_path, "error": str(e)}], args.failed_text)

def test_one_file():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", type=str, required=True, help="detect one file")
    args = parser.parse_args()
    extension = detect_file_type(args.file_path)
    print(extension)


if __name__ == "__main__":
    main()
    # test_one_file()

        # detected_ext = detect_file_type(filename)
        # if detected_ext is not None:
        #     rename_file_with_extension(filename, detected_ext
