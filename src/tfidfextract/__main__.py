import os
from math import log, e
from pathlib import Path
from itertools import cycle

import polars as pl
import xlsxwriter
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from tfidfextract import BASE_DIR
from tfidfextract.utils import load_data
from tfidfextract.process import process_single, extract_idf, tf_to_tfidf_sum


def main():
    data = load_data()

    result_map: dict[str, list[tuple[str, str, Path]]] = {}
    idf_map: dict[str, dict[str, int]] = {}

    tf_idf: dict[str, list[dict]] = {}

    # Stage1: extract tf
    for year, file_list in data.items():
        result_map[year] = process_map(
            process_single, file_list, chunksize=32, desc=f"TF_{year}"
        )

    # Stage2: calculate IDF
    for year, result_list in result_map.items():
        idf_path = os.path.join(BASE_DIR, f"{year}-idf.parquet")
        if not os.path.isfile(idf_path):
            idf_df = extract_idf(result_list, year)
            idf_df.write_parquet(idf_path)
        else:
            idf_df = pl.read_parquet(idf_path)
        idf_dict = next(idf_df.iter_rows(named=True))
        idf_map[year] = idf_dict

    # Stage3: calculate sum of TF-IDF
    for year, tf_results in result_map.items():
        idf_dict = idf_map[year]
        result_num = len(tf_results)
        expressions = [
            pl.col(keyword)
            .cast(pl.Float64)
            .add(1.0)
            .log()
            .mul(log(result_num / (count + 1), e))
            for keyword, count in idf_dict.items()
        ]

        tf_idf[year] = process_map(
            tf_to_tfidf_sum,
            zip(tf_results, cycle([expressions])),
            chunksize=32,
            desc=f"TF-IDF_{year}",
        )

    # Stage4: write output file
    with xlsxwriter.Workbook("result.xlsx") as wb:
        for year, sheet in tqdm(tf_idf.items(), desc="Output"):
            sheet = pl.DataFrame(sheet)
            sheet.write_excel(workbook=wb, worksheet=year)


if __name__ == "__main__":
    main()
