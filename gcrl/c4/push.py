from datasets import Dataset, load_dataset
from tqdm import tqdm
import requests
import gzip
import json
import os


def generate_c4():
    cache_dir = "/tmp/huggingface/datasets"

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    habr_dataset = load_dataset("IlyaGusev/habr", cache_dir=cache_dir)

    for item in habr_dataset['train']:
        yield {"text": item['text_markdown'], "source": "habr"}

    ru_wiki_dataset = load_dataset("wikipedia", "20220301.ru", cache_dir=cache_dir)

    for item in ru_wiki_dataset['train']:
        yield {"text": item['text'], "source": "wikipedia-ru"}

    en_wiki_dataset = load_dataset("wikipedia", "20220301.en", cache_dir=cache_dir)

    for item in en_wiki_dataset['train']:
        yield {"text": item['text'], "source": "wikipedia-en"}

    proza_dataset = load_dataset("danasone/taiga_proza", cache_dir=cache_dir)

    for item in proza_dataset['train']:
        yield {"text": item['text'], "source": "proza"}

    openwebtext_dataset = load_dataset("Skylion007/openwebtext", cache_dir=cache_dir)

    for item in openwebtext_dataset['train']:
        yield {"text": item['text'], "source": "openwebtext-en"}

    stihiru_dataset = load_dataset("IlyaGusev/stihi_ru", cache_dir=cache_dir)

    for item in stihiru_dataset['train']:
        yield {"text": item['text'], "source": "stihi_ru"}

    ru_news = load_dataset("IlyaGusev/ru_news", cache_dir=cache_dir)

    for item in ru_news['train']:
        yield {"text": item['text'], "source": "ru_news"}

    pikabu = load_dataset("IlyaGusev/pikabu", cache_dir=cache_dir)

    for item in pikabu['train']:
        yield {"text": item['text_markdown'], "source": "pikabu"}

    flibusta = load_dataset("0x7o/GCRL-flibusta", cache_dir=cache_dir)

    for item in flibusta['train']:
        yield {"text": item['text'], "source": "flibusta"}

    # Remove cache_dir to free up space
    os.rmdir(cache_dir)

    for n in tqdm(range(2326)):
        url = f"https://storage.yandexcloud.net/0x7o/c4-ru-cleaned/c4-cleaned-{n:05}.json.gz"
        response = requests.get(url)
        decompressed_data = gzip.decompress(response.content)
        data = json.loads(decompressed_data.decode())

        for text in data['text']:
            yield {"text": text, "source": "c4-ru"}


dataset = Dataset.from_generator(generate_c4)
dataset.shuffle()
dataset.push_to_hub("Aeonium/the-data")
