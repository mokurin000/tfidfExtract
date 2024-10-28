import os


BASE_DIR = "./data"
target_dirs = map(lambda d: os.path.join(BASE_DIR, d), os.listdir(BASE_DIR))
