import os
from math import log, e
from pathlib import Path
from itertools import cycle
from multiprocessing import Pool

import polars as pl
import xlsxwriter

from tfidfextract import BASE_DIR
from tfidfextract.utils import load_data
from tfidfextract.process import process_single, extract_idf, tf_to_tfidf_sum


def main():
    data = load_data()

    result_map: dict[str, list[tuple[str, str, Path]]] = {}
    idf_map: dict[str, dict[str, int]] = {}

    tf_idf: dict[str, list[dict]] = {}

    with Pool() as pool:
        # Stage1: extract tf
        for year, file_list in data.items():
            result_map[year] = pool.map(process_single, file_list)

        # Stage2: calculate IDF
        for year, result_list in result_map.items():
            idf_path = os.path.join(BASE_DIR, f"{year}-idf.parquet")
            if not os.path.isfile(idf_path):
                idf_df = extract_idf(result_list, pool)
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

            tf_idf[year] = pool.map(
                tf_to_tfidf_sum,
                zip(tf_results, cycle([expressions])),
            )

    # Stage4: write output file
    with xlsxwriter.Workbook("result.xlsx") as wb:
        for year, sheet in tf_idf.items():
            sheet = pl.DataFrame(sheet)
            sheet.write_excel(workbook=wb, worksheet=year)


if __name__ == "__main__":
    main()
