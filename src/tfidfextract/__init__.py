import os


BASE_DIR = "./data"
target_dirs = filter(
    lambda p: os.path.isdir(p),
    map(lambda d: os.path.join(BASE_DIR, d), os.listdir(BASE_DIR)),
)
