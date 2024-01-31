import json

from utils import calculate_entropy
from chunks import C4Chunks
from tqdm import tqdm
import os

c4 = C4Chunks()
output_dir = "./c4-cleaned"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def main():
    for chunk_num in tqdm(range(4096)):
        if os.path.exists(os.path.join(output_dir, f"c4-cleaned-{chunk_num:05}.json")):
            continue

        data = {"text": []}
        c4.download_chunk(chunk_num)

        for item in tqdm(c4.iter_text(chunk_num)):
            text = item["text"]
            entropy = calculate_entropy(text)

            if entropy < 4.6 or entropy >= 4.7:
                continue

            data["text"].append(text)

        with open(
                os.path.join(output_dir, f"c4-cleaned-{chunk_num:05}.json"),
                "w",
                encoding="utf-8",
        ) as f:
            json.dump(data, f, indent=4)


if __name__ == "__main__":
    main()
