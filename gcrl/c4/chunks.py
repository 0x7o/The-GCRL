import os
import glob
import json
import gzip
import requests
from tqdm import tqdm


class C4Chunks:
    def __init__(self, temp_dir="/tmp"):
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        self.temp_dir = temp_dir
        self.chunks = glob.glob(
            os.path.join(self.temp_dir, "c4-ru.tfrecord-*-of-04096.json.gz")
        )

    def get_filename(self, chunk_num: int) -> str:
        return os.path.join(
            self.temp_dir, f"c4-ru.tfrecord-{chunk_num:05}-of-04096.json.gz"
        )

    def download_chunk(self, chunk_num: int) -> None:
        if os.path.exists(self.get_filename(chunk_num)):
            return

        url = f"https://huggingface.co/datasets/allenai/c4/resolve/main/multilingual/c4-ru.tfrecord-{chunk_num:05}-of-04096.json.gz?download=true"
        response = requests.get(url, stream=True)
        file_name = os.path.join(
            self.temp_dir, f"c4-ru.tfrecord-{chunk_num:05}-of-04096.json.gz"
        )
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)

        with open(file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                progress_bar.update(len(chunk))
                if chunk:
                    file.write(chunk)

        progress_bar.close()

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ОШИБКА, что-то пошло не так")

        print(f"Скачал чанк {chunk_num} в {file_name}")

    def iter_text(self, chunk_num: int) -> dict:
        filename = self.get_filename(chunk_num)

        if not os.path.exists(filename):
            assert False, f"Нужно скачать чанк {chunk_num}"

        with gzip.open(filename, "r") as f:
            lines = f.readlines()
            for line in lines:
                yield json.loads(line)

    def remove_chunk(self, chunk_num: int) -> None:
        os.remove(self.get_filename(chunk_num))
