"""
Process all the files in a LaTeX zip file to extract paper content

1. Unzips LaTeX ZIP file
2. Identifies primary TEX file
3. Expands other TEX files into main TEX file using latexpand
4. Expands BBL file into main TEX file
5. Convert TEX file into XML using tralics
6. Extract content of XML into S2ORC JSON

"""

import os
import gzip
import tarfile
import zipfile
import shutil
from typing import Optional

from doc2json.utils.latex_util import normalize, latex_to_xml, latex_to_html


def _is_gzip_file(fpath):
    with open(fpath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'


def extract_latex(zip_file: str, latex_dir: str, cleanup=True):
    """
    Unzip latex zip into temp directory
    :param zip_file:
    :param latex_dir:
    :param cleanup:
    :return:
    """
    assert os.path.exists(zip_file)
    # assert zip_file.endswith('.gz') or zip_file.endswith('.zip') or zip_file.endswith('.tar') or zip_file.endswith('.tar.gz')
    assert zip_file.endswith('.tar.gz') or zip_file.endswith('.tar') or zip_file.endswith('.gz') or zip_file.endswith('.zip') or zip_file.endswith('.tex')

    # get name of zip file
    file_id = os.path.splitext(zip_file)[0].split('/')[-1]

    # check if tar file -> untar
    tar_dir = os.path.join(latex_dir, file_id)
    os.makedirs(tar_dir, exist_ok=True)
    if tarfile.is_tarfile(zip_file):
        with tarfile.open(zip_file) as tar:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, tar_dir)
    # check if gzip file -> un-gz and/or untar
    elif _is_gzip_file(zip_file):
        tar_file = os.path.join(latex_dir, f'{file_id}.tar')
        with gzip.open(zip_file, 'rb') as in_f, open(tar_file, 'wb') as out_f:
            s = in_f.read()
            out_f.write(s)
        if os.path.exists(tar_file):
            # check if tarfile
            if tarfile.is_tarfile(tar_file):
                with tarfile.open(tar_file) as tar:
                    def is_within_directory(directory, target):
                        
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                    
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        
                        return prefix == abs_directory
                    
                    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                    
                        for member in tar.getmembers():
                            member_path = os.path.join(path, member.name)
                            if not is_within_directory(path, member_path):
                                raise Exception("Attempted Path Traversal in Tar File")
                    
                        tar.extractall(path, members, numeric_owner=numeric_owner) 
                        
                    
                    safe_extract(tar, tar_dir)
                os.remove(tar_file)
            # else, copy to tex file
            else: # old tex file could be ps file
                tex_file = os.path.join(latex_dir, file_id, f'{file_id}.tex')
                os.makedirs(tar_dir, exist_ok=True)
                os.rename(tar_file, tex_file)
    # check if zip file -> unzip
    elif zipfile.is_zipfile(zip_file):
        with zipfile.ZipFile(zip_file, 'r') as in_f:
            in_f.extractall(tar_dir)
    elif zip_file.endswith('.tex'):
        tex_file = os.path.join(latex_dir, file_id, f'{file_id}.tex')
        os.makedirs(tar_dir, exist_ok=True)
        os.rename(zip_file, tex_file)
    else:
        return None

    # clean up if needed
    if cleanup:
        os.remove(zip_file)

    # returns directory
    if os.path.exists(tar_dir):
        return tar_dir


def normalize_latex(latex_dir: str, norm_dir: str, norm_log_file: str, cleanup=True) -> (str, str):
    """
    Normalize all latex files from arxiv
    :param latex_dir:
    :param norm_dir:
    :param norm_log_file:
    :param cleanup:
    :return:
    """
    # normalize file
    file_id = latex_dir.strip('/').split('/')[-1]
    if file_id == 'skipped':
        return None
    norm_output_folder = os.path.join(norm_dir, file_id)
    os.makedirs(norm_output_folder, exist_ok=True)

    try:
        main_tex_fn = normalize(latex_dir, norm_output_folder)
    except TypeError:
        shutil.rmtree(norm_output_folder)
        with open(norm_log_file, 'a+') as log_f:
            log_f.write(f'{file_id}\n')

    # delete latex directory if cleanup
    if cleanup:
        shutil.rmtree(latex_dir)

    return norm_output_folder, main_tex_fn


def norm_latex_to_xml(norm_dir: str, xml_dir: str, xml_err_file: str, xml_log_file: str, cleanup=True) -> Optional[str]:
    """
    Convert LaTeX to XML using tralics
    :param norm_dir:
    :param xml_dir:
    :param xml_err_file:
    :param xml_log_file:
    :param cleanup:
    :return:
    """
    file_id = norm_dir.strip('/').split('/')[-1]
    norm_tex_file = os.path.join(norm_dir, f'{file_id}.tex')
    xml_output_dir = os.path.join(xml_dir, file_id)
    xml_file = os.path.join(xml_output_dir, f'{file_id}.xml')
    os.makedirs(xml_output_dir, exist_ok=True)

    latex_to_xml(
        tex_file=norm_tex_file,
        out_dir=xml_output_dir,
        out_file=xml_file,
        err_file=xml_err_file,
        log_file=xml_log_file
    )

    # delete norm directory if cleanup
    if cleanup:
        shutil.rmtree(norm_dir)
    if os.path.exists(xml_file):
        return xml_file


def convert_latex_to_xml(
        zip_file: str, latex_dir: str, norm_dir: str, xml_dir: str, html_dir: str, log_dir: str, cleanup=True
) -> (str, str, str):
    """
    Run expansion, normalization, xml conversion on latex
    :param zip_file:
    :param latex_dir:
    :param norm_dir:
    :param xml_dir:
    :param log_dir:
    :param cleanup:
    :return:
    """
    # extract zip file
    latex_output_dir = extract_latex(zip_file, latex_dir, cleanup)
    # normalize latex
    norm_log_file = os.path.join(log_dir, 'norm_error.log')
    norm_output_dir, main_tex_fn = normalize_latex(latex_output_dir, norm_dir, norm_log_file, cleanup)

    # convert to xml
    xml_error_file = os.path.join(log_dir, 'xml_error.log')
    xml_log_file = os.path.join(log_dir, 'xml_skip.log')

    html_error_file = os.path.join(log_dir, 'html_error.log')
    html_log_file = os.path.join(log_dir, 'html_skip.log')
    xml_output_file = norm_latex_to_xml(norm_output_dir, xml_dir, xml_error_file, xml_log_file, cleanup)
    if xml_output_file is None:
        return None, None, None
    html_output_file = norm_latex_to_html(main_tex_fn, html_dir, html_error_file, html_log_file)
    return xml_output_file, html_output_file, main_tex_fn

def norm_latex_to_html(main_tex_file: str, html_dir: str, html_err_file: str, html_log_file: str) -> Optional[str]:
    """
    Convert LaTeX to XML using tralics
    :param norm_dir:
    :param xml_dir:
    :param xml_err_file:
    :param xml_log_file:
    :param cleanup:
    :return:
    """
    file_id = main_tex_file.strip('/').split('/')[-2]
    norm_tex_file = main_tex_file
    html_output_dir = os.path.join(html_dir, file_id)
    html_file = os.path.join(html_output_dir, f'{file_id}_latexml.html')
    os.makedirs(html_output_dir, exist_ok=True)
    latex_to_html(
        tex_file=norm_tex_file,
        out_file=html_file,
        err_file=html_err_file,
        log_file=html_log_file
    )
    if os.path.exists(html_file):
        return html_file


def convert_latex_to_s2orc_json(
        latex_zip: str,
        base_temp_dir: str,
        cleanup_after: bool=True
) -> (str, str, str):
    """
    Convert a LaTeX zip file to S2ORC JSON
    :param latex_zip:
    :param base_temp_dir:
    :param cleanup_after:
    :return:
    """
    if not os.path.exists(latex_zip):
        raise FileNotFoundError("Input LaTeX ZIP file doesn't exist")

    # temp directories
    latex_expand_dir = os.path.join(base_temp_dir, 'latex')
    latex_norm_dir = os.path.join(base_temp_dir, 'norm')
    latex_xml_dir = os.path.join(base_temp_dir, 'xml')
    latex_html_dir = os.path.join(base_temp_dir, 'html')
    latex_log_dir = os.path.join(base_temp_dir, 'log')

    os.makedirs(base_temp_dir, exist_ok=True)
    os.makedirs(latex_expand_dir, exist_ok=True)
    os.makedirs(latex_norm_dir, exist_ok=True)
    os.makedirs(latex_xml_dir, exist_ok=True)
    os.makedirs(latex_log_dir, exist_ok=True)
    # convert to XML
    xml_file, html_file, main_tex_fn = convert_latex_to_xml(
        latex_zip, latex_expand_dir, latex_norm_dir, latex_xml_dir, latex_html_dir, latex_log_dir, cleanup_after
    )
    return xml_file, html_file, main_tex_fn
