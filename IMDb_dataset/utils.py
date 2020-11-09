import json
import logging
import os
import shutil
import sys


from tqdm import tqdm
import requests


def download_file(url, output_path, exist_overwrite, min_size=0, verbose=True):
    # Todo handle requests.exceptions.ConnectionError
    if exist_overwrite or not os.path.exists(output_path):
        r = requests.get(url, stream=True)
        total_size = int(r.headers.get('content-length', 0))
        size_read = 0
        if total_size - min_size > 0:
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                disable=not verbose
            ) as pbar:
                with open(output_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            size_read = min(total_size, size_read + 1024)
                            pbar.update(len(chunk))

