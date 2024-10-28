import os
from pathlib import Path
from multiprocessing.pool import Pool

import polars as pl
from polars import Expr
from tfidfextract.process.keywords import KEYWORDS


def extract_tf(filepath: Path | str) -> pl.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    df = pl.DataFrame([{keyword: content.count(keyword) for keyword in KEYWORDS}])
    return df


def extract_idf_single(result: tuple[str, str, Path]) -> dict[str, int]:
    _, _, pqpath = result
    try:
        df = pl.read_parquet(pqpath)
    except Exception:
        print("failed to load from", pqpath)
        exit(1)

    dict_ = next(
        df.with_columns(
            pl.col(KEYWORDS).map_elements(
                lambda count: 1 if count > 0 else 0, return_dtype=pl.Int32
            )
        ).iter_rows(named=True)
    )
    return dict_


def extract_idf(results: list[tuple[str, str, Path]], pool: Pool) -> pl.DataFrame:
    idf_counts = pool.map(extract_idf_single, results)
    idf = pl.DataFrame(idf_counts).sum()
    return idf


def process_single(pair: tuple[str, str, str]) -> tuple[str, str, Path]:
    stock, name, path = pair

    parquet_path = path.replace(".txt", ".parquet")
    if not os.path.isfile(parquet_path):
        tf = extract_tf(path)
        tf.write_parquet(parquet_path)

    return (stock, name, Path(parquet_path))


def tf_to_tfidf_sum(in_data: tuple[tuple[str, str, Path], list[Expr]]) -> dict:
    result, expressions = in_data
    stock, name, tf_df = result
    tf_idf_df = pl.read_parquet(tf_df).with_columns(expressions)
    info = next(tf_idf_df.iter_rows(named=True))
    tf_idf_value = sum(info.values())
    return {
        "stock": stock,
        "name": name,
        "tf-idf": tf_idf_value,
    }
