import os
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor



# Define headers
headers = {
    'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    'accept-language': 'zh-HK,zh;q=0.9,en-HK;q=0.8,en-US;q=0.7,en;q=0.6,fr;q=0.5,zh-TW;q=0.4',
    'cache-control': 'no-cache',
    'cookie': 'cf_clearance=tCgp593_he0SqdU6Ch4wq7OG7SoBhm_TJLjjhk22SzM-1712844462-1.0.1.1-d60nA25nSl6NNpk8ysNrOcXSaD2vSsH5Y6nRDJSv4C8cfj_qAVK33Zw9DlLQd1D6KtyOIQ1QrFZ4mobJx8q5xQ',
    'pragma': 'no-cache',
    'referer': 'https://tw.8se.me/',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'image',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}

"""
 Read URLs from file
with open("urlinput.txt", "r") as file:
    urls = file.read().splitlines()

for i, url in enumerate(urls, start=1):
    filename = f"image{i:03}.jpg"
    print(f"Starting download of {url} to {filename}")

    # Send the request
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Write content into image file
        with open(filename, "wb") as img_file:
            img_file.write(response.content)
        print(f"Download of {filename} complete")
    else:
        print(f"Failed to download {url}. HTTP response code: {response.status_code}")
"""

"""
# Define base URL
base_url = "https://img.xchina.biz/photos/65a5997b85664/"

for i in range(1, 194):
    # create padding for numbers under 10
    if i < 10:
        filename = f"image00{i}.jpg"
        full_url = f'{base_url}000{i}.jpg'
    elif i < 100:
        filename = f"image0{i}.jpg"
        full_url = f'{base_url}00{i}.jpg'
    else:
        filename = f"image{i}.jpg"
        full_url = f'{base_url}0{i}.jpg'
    
    print(f"Starting download of image number {i} to {filename}")

    # Send the request
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
        # Write content into image file
        with open(filename, "wb") as img_file:
            img_file.write(response.content)
        print(f"Download of {filename} complete")
    else:
        print(f"Failed to download {full_url}. HTTP response code: {response.status_code}")
"""

"""
def download_image(i):
    # prepare padding for the filename
    filename = f"vol3/image{i:03}.jpg"
    full_url = f'https://img.xchina.biz/photos/65ada9b725e6f/{i:04}.jpg'

#https://img.xchina.biz/photos/65c8fd9f1bf06/0001.jpg
#65ada9b725e6f

    # Request the URL
    response = requests.get(full_url, headers=headers, stream=True)
    if response.status_code == 200:
        # Get the file size from headers
        file_size = int(response.headers.get("Content-Length", 0))
        progress_bar = tqdm(total=file_size, unit='iB', unit_scale=True)

        # Write content to file
        with open(filename, "wb") as output_file:
            for data in response.iter_content(chunk_size=1024):
                progress_bar.update(len(data))
                output_file.write(data)
        progress_bar.close()

        if file_size != 0 and progress_bar.n != file_size:
            print("ERROR, something went wrong while downloading.")
        else:
            print(f"{filename} download complete!")
    else:
        print(f"Failed to download {full_url}. HTTP response code: {response.status_code}")

# Multithread downloads
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(download_image, range(1, 92))        
"""

def download_image(i, pbar):
    # prepare padding for the filename
    filename = f"vol5/image{i:03}.jpg"  # change here
    full_url = f'https://img.xchina.biz/photos/64bd67d99131b/{i:04}.jpg'
    
    #https://img.xchina.biz/photos/64bd67d99131b/0001.jpg

    response = requests.get(full_url, headers=headers, stream=True)
    
    if response.status_code == 200:
        # Write content to file
        with open(filename, "wb") as output_file:
            for data in response.iter_content(chunk_size=1024):
                output_file.write(data)
        pbar.update(1)
    else:
        print(f"Failed to download {full_url}. HTTP response code: {response.status_code}")


with ThreadPoolExecutor(max_workers=5) as executor, tqdm(total=86) as pbar:
    for i in range(1, 86):
        executor.submit(download_image, i, pbar)