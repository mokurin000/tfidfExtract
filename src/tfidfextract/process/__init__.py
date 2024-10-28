import os
from pathlib import Path

import polars as pl
from tfidfextract.process.keywords import KEYWORDS


def extract_tf(filepath: Path | str) -> pl.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    df = pl.DataFrame([{keyword: content.count(keyword) for keyword in KEYWORDS}])
    return df


def process_single(pair: tuple[str, str, str]):
    stock, name, path = pair

    parquet_path = path.replace(".txt", ".parquet")
    if not os.path.isfile(parquet_path):
        tf = extract_tf(path)
        tf.write_parquet(parquet_path)

    return (stock, name, parquet_path)
