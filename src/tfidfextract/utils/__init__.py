import os
from pathlib import Path

from tfidfextract import target_dirs


def load_data(extension=".txt") -> dict[str, tuple[str, str, Path]]:
    data = {}
    for target_dir in target_dirs:
        files = os.listdir(target_dir)
        for file in files:
            stock_code, year, name = file.split("_")[:3]
            path = Path(os.path.join(target_dir, file))
            if path.suffix != extension:
                continue
            data[year] = data.get(year, []) + [(stock_code, name, path)]

    return data
