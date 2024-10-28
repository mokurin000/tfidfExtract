import os

from tfidfextract import target_dirs


def load_data() -> dict:
    data = {}
    for target_dir in target_dirs:
        files = os.listdir(target_dir)
        for file in files:
            stock_code, year, name = file.split("_")[:3]
            path = os.path.join(target_dir, file)
            data[year] = data.get(year, []) + [(stock_code, name, path)]

    return data
