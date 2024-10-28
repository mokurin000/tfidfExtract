import os
from multiprocessing import Pool

import polars as pl

from tfidfextract import BASE_DIR
from tfidfextract.utils import load_data
from tfidfextract.process import process_single, extract_idf


def main():
    data = load_data()
    result = {}
    with Pool() as pool:
        for year, file_list in data.items():
            result[year] = pool.map(process_single, file_list)

        for year, result_list in result.items():
            idf_path = os.path.join(BASE_DIR, f"{year}-idf.parquet")
            if not os.path.isfile(idf_path):
                idf_df = extract_idf(result_list, pool)
                idf_df.write_parquet(idf_path)
            else:
                idf_df = pl.read_parquet(idf_path)


if __name__ == "__main__":
    main()
