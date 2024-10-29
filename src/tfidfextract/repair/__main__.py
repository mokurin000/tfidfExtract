from os import path, listdir
from pathlib import Path

from tqdm.contrib.concurrent import process_map

from tfidfextract import BASE_DIR
from tfidfextract.utils import load_data
from tfidfextract.repair.payload import validate_single_pair, validate_single_path


def main():
    data = load_data(extension=".parquet")
    for year, file_list in data.items():
        process_map(validate_single_pair, file_list, desc=f"{year}", chunksize=32)

    idf_files = filter(
        lambda p: p.suffix == ".parquet",
        map(
            Path,
            filter(
                path.isfile,
                map(lambda filename: path.join(BASE_DIR, filename), listdir(BASE_DIR)),
            ),
        ),
    )
    process_map(validate_single_path, idf_files, desc="IDF", chunksize=32)


if __name__ == "__main__":
    main()
