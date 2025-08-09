import os
import glob
import codecs
import chardet
from loguru import logger
# 
#    '': '', 
REPLACEMENTS = {
    '麯': '曲',
    '隻': '只',
    '鞦': '秋',
    '傢': '家',    
    '韆': '千',
    '迴': '回', 
    '齣': '出', 
    '彆': '別',
    '瞭': '了', 
    '嚮': '向', 
    '發': '髮', 
    '閤': '合', 
    '颱': '台',   
    '榖': '谷',     
    '麵': '面'
}

logger.add("file_{time}.log")

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())  
    enc = result['encoding']
    logger.info("Detected encoding: " + enc)
    # Test if the encoding works
    try:
        with codecs.open(file_path, 'r', encoding=enc) as file:
            file.read()
    except UnicodeDecodeError as e:
        #logger.error(f"Failed to open file with detected encoding : {file_path}, {e}")
        # Try opening with 'gbk'
        try:
            with codecs.open(file_path, 'r', encoding='gbk') as file:
                file.read()
            enc = 'gbk'
        except UnicodeDecodeError as e:
            # Try opening with 'gb18030'
            try:
                with codecs.open(file_path, 'r', encoding='gb18030') as file:
                    file.read()
                enc = 'gb18030'
            except UnicodeDecodeError as e:
                logger.error(f"Failed to open file with 'gb18030' and 'gbk' encoding : {file_path}, {e}")
                enc = 'utf-8'  # default to utf-8 if 'gb2312' also fails
    
    return enc

def rename_files(directory):
    logger.info("rename_files: working directory: " + directory)
    for filename in glob.glob(os.path.join(directory, '**'), recursive=True):
        logger.info("rename_files: found file: " + filename)
        if os.path.isfile(filename):
            new_filename = os.path.basename(filename)
            for src, target in REPLACEMENTS.items():
                new_filename = new_filename.replace(src, target)
            
            if new_filename != os.path.basename(filename):
                os.rename(filename, os.path.join(os.path.dirname(filename), new_filename))
                logger.debug(f"Renamed file: {filename} to {os.path.join(os.path.dirname(filename), new_filename)}")

def replace_in_files(directory):
    # logger.info(f"replace_in_files: working in: {directory}")
    for root, dirs, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            # logger.info(f"replace_in_files: found file: {filepath}")
            if filepath.endswith('.cue') or filepath.endswith('.txt'):
                encoding = detect_encoding(filepath)
                with codecs.open(filepath, 'r', encoding) as file:
                    content = file.read()
                for src, target in REPLACEMENTS.items():
                    content = content.replace(src, target)
                with codecs.open(filepath, 'w', encoding) as file:
                    file.write(content)
                logger.debug(f"Replaced content (if found) in file: {filepath}")

def rename_directories(directory):
    for dirname in glob.glob(os.path.join(directory, '**'), recursive=True):
        if os.path.isdir(dirname):
            new_dirname = dirname
            for src, target in REPLACEMENTS.items():
                new_dirname = new_dirname.replace(src, target)
            if new_dirname != dirname:
                os.rename(dirname, new_dirname)
                logger.debug(f"Renamed directory: {dirname} to {new_dirname}")

def process_directory(directory):
    rename_files(directory)
    replace_in_files(directory)
    rename_directories(directory)


directory_path = 'D:/BaiduNetdiskDownload/杜德偉/'

logger.info("started")
process_directory(directory_path)



