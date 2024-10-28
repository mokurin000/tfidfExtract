from multiprocessing import Pool

from tfidfextract.utils import load_data
from tfidfextract.process import process_single


def main():
    data = load_data()
    result = {}
    with Pool() as pool:
        for year, file_list in data.items():
            result[year] = pool.map(process_single, file_list)
    print(result)


if __name__ == "__main__":
    main()
