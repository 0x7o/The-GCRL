import json
import os.path

from datasets import Dataset
import glob

input_dir = "c4-cleaned"
data = {"text": []}

for file in glob.glob(os.path.join(input_dir, "*.json")):
    with open(file, "r", encoding="utf-8") as f:
        items = json.load(f)
    data["text"].extend(items["text"])

dataset = Dataset.from_dict(data)
dataset.push_to_hub("Aeonium/c4-ru-cleaned")
