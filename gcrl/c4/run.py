import gzip
import json

from utils import calculate_entropy
from chunks import C4Chunks
from tqdm import tqdm
import boto3
import os

c4 = C4Chunks()
output_dir = "./c4-cleaned"
session = boto3.session.Session(
    aws_access_key_id=os.environ["S3_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET"],
)
s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def main():
    for chunk_num in tqdm(range(2327, 4096)):
        if os.path.exists(os.path.join(output_dir, f"c4-cleaned-{chunk_num:05}.json")):
            continue

        data = {"text": []}
        c4.download_chunk(chunk_num)

        for item in tqdm(c4.iter_text(chunk_num)):
            text = item["text"]
            entropy = calculate_entropy(text)

            if entropy < 4.6 or entropy >= 4.7:
                continue

            if len(text) < 512:
                continue

            data["text"].append(text)

        file_name = os.path.join(output_dir, f"c4-cleaned-{chunk_num:05}.json.gz")

        with gzip.open(
            file_name,
            "wb",
        ) as f:
            f.write(json.dumps(data).encode("utf-8"))

        c4.remove_chunk(chunk_num)

        with open(file_name, "rb") as f:
            s3.upload_fileobj(
                f, "0x7o", "c4-ru-cleaned/" + f"c4-cleaned-{chunk_num:05}.json.gz"
            )

        os.remove("c4-ru-cleaned/" + f"c4-cleaned-{chunk_num:05}.json.gz")


if __name__ == "__main__":
    main()
