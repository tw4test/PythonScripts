import os
import codecs
import glob
import chardet
from loguru import logger
from hanziconv import HanziConv

#logger.add("batchconvert_{time}.log")
logger.add("_batchconvert.log")

#######################
# 1. Python that convert file names and folder names from Simplified Chinese characters to Traditional Chinese characters.
# 2. It traverse a specified directory including its subdirectories.
# 3. After the conversion, the script should also check the encoding of .txt and .cue files and convert the content to Traditional Chinese characters as necessary.
# 4. If the encoding of these files is not 'UTF-8', it should convert the encoding to 'UTF-8'.
# 5. The conversion process should not throw any errors. If a file or folder name is already in Traditional Chinese, it should be skipped without causing any issues.
# 6. The whole process should be logged for debugging purposes.
# 7. Run process_directory to rename file/folder name and file contents that is converted with incorrect meanings

#1. python 脚本将文件名和文件夹名从简体中文字符转换为繁体中文字符。
#2. 它遍历指定的目录及其子目录。
#3. 在转换完成后，脚本还应检查 .txt 和 .cue 文件的编码，并根据需要将内容转换为繁体中文字符。
#4. 如果这些文件的编码不是 'UTF-8'，则应将编码转换为 'UTF-8'。
#5. 转换过程不应抛出任何错误。如果文件或文件夹名称已经是繁体中文，则应跳过而不引起任何问题。
#6. 整个过程应为调试目的记录日志。
#7. 运行 process_directory 以重命名文件/文件夹名称以及转换含有错误含义的文件内容。
#######################

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
    '麵': '面',
    '闆': '板',
    '鍾': '鐘',
    '誌': '志',
    '矸': '干',
    '鬆': '松',
    '纔': '才',
    '鼕': '冬',
    '剋': '克',
    '係': '系',
    '睏': '困',
    '範': '范'
}

def is_traditional(text):
    return text == HanziConv.toTraditional(text)

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())  
    enc = result['encoding']
    if enc is not None:
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

def convert_and_save(file_path, encoding):
    with codecs.open(file_path, 'r', encoding=encoding) as file:
        content = file.read()
    if not is_traditional(content):
        logger.debug(f"Converting content of file: {file_path}")
        content = HanziConv.toTraditional(content)
    with codecs.open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)


def convert_to_traditional(root_dir):
    for dir_path, dir_names, file_names in os.walk(root_dir , topdown=False):
        logger.info(f"111111111111111111111111111111111111111111111111111111111")        
        logger.info(f"Working on directory: {dir_path}")

        for file_name in file_names:
            # logger.info(f"file found: {file_name}")
            new_file_name = HanziConv.toTraditional(file_name)
            old_file_path = os.path.join(dir_path, file_name)
            new_file_path = os.path.join(dir_path, new_file_name)

            if file_name != new_file_name:
                os.rename(old_file_path, new_file_path)
                logger.debug(f"Renamed file to: {new_file_name}")
                #logger.debug(f"Renamed file: {old_file_path} to {new_file_path}")

            if new_file_path.endswith('.txt') or new_file_path.endswith('.cue'): #Convert the content anyways
                # logger.info(f"Checking encoding of file: {new_file_path}")
                encoding = detect_encoding(new_file_path)
                # if encoding != 'utf-8':
                    # logger.debug(f"Convertingfile: {new_file_path} from encoding: {encoding} to UTF-8.")
                convert_and_save(new_file_path, encoding)

        for dir_name in dir_names:
            old_dir_path = os.path.join(dir_path, dir_name)
            new_dir_name = HanziConv.toTraditional(dir_name)
            new_dir_path = os.path.join(dir_path, new_dir_name)

            if dir_name != new_dir_name:
                logger.debug(f"Converting directory name: {dir_name} to {new_dir_name}")
                os.rename(old_dir_path, new_dir_path)
                #logger.debug(f"Renamed directory: {old_dir_path} to {new_dir_path}")


def rename_files(directory):
    logger.info(f"22222222222222222222222222222222222222222222222222")          
    # logger.info(f"Working on directory: {directory}")
    # file_count = len(glob.glob(os.path.join(directory, '**'), recursive=True))
    # logger.info(f"Number of files found: {file_count}")   
    for root, dirs, files in os.walk(directory , topdown=False):
        for filename in files:
            filepath = os.path.join(root, filename)
            if os.path.isfile(filepath):
                new_filename = os.path.basename(filepath)
                for src, target in REPLACEMENTS.items():
                    new_filename = new_filename.replace(src, target)
                
                if new_filename != os.path.basename(filepath):
                    os.rename(filepath, os.path.join(root, new_filename))
                    logger.debug(f"Renamed file: {filepath} to {os.path.join(root, new_filename)}")

def replace_in_files(directory):
    logger.info(f"3333333333333333333333333333333333333333333333333333333333333")          
    # logger.info(f"Working on directory: {directory}")
    for root, dirs, files in os.walk(directory , topdown=False):
        for filename in files:
            filepath = os.path.join(root, filename)
            # logger.info(f"replace_in_files: found file: {filepath}")
            if filepath.endswith('.cue') or filepath.endswith('.txt'):
                encoding = detect_encoding(filepath)
                with codecs.open(filepath, 'r', encoding=encoding) as file:
                    content = file.read()
                for src, target in REPLACEMENTS.items():
                    content = content.replace(src, target)
                with codecs.open(filepath, 'w', encoding='utf-8') as file:
                    file.write(content)
                logger.debug(f"Replaced content (if found) in file: {filepath}")

def rename_directories(directory):
    logger.info(f"4444444444444444444444444444444444444444444444444444444444")        
    for root, dirs, files in os.walk(directory , topdown=False):
        for dirname in dirs:
            new_dirname = dirname
            for src, target in REPLACEMENTS.items():
                new_dirname = new_dirname.replace(src, target)
            if new_dirname != dirname:
                os.rename(os.path.join(root, dirname), os.path.join(root, new_dirname))
            logger.debug(f"Renamed directory: {dirname} to {new_dirname}")

def process_directory(directory):
    rename_files(directory)
    replace_in_files(directory)
    rename_directories(directory)  

# Confirm the directory path
directory_path = input("Please enter the directory path: ")
print(f"Process: {directory_path}")
#confirmation = input("Is this correct? (yes/no): ")

#while confirmation.lower() not in ['yes', 'y']:
 #   directory_path = input("Please enter the directory path: ")
 #   print(f"Process: {directory_path}")
 #   confirmation = input("Is this correct? (yes/no): ")

# directory_path = 'Q:\女歌手'

convert_to_traditional(directory_path)
process_directory(directory_path)

logger.info("Done Done Done")