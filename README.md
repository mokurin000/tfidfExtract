# TF-IDF Extract


## Usage

```bash
python -m tfidfextract
```

## Performance

Processed 49119 text files, ~19439MiB in total in 20m56s

Environment:
```text
OS: Windows 11, 22631
CPU: i7-12700H (6P8E)
RAM: 2*8GB DDR5 4800
SSD: Predator GM7000 2TB
```

If you have enough RAM, try place data to RamDisk (Windows) or ramfs mount point (Max/Linux).

This program will save intermediate data on disk, to reduce RAM usage.

But once you interrupt this program and resume, some data might be broken

In this case you should delete corrupted files:

```bash
python -m tfidfextract.repair
```

## Installation

```bash
pip install -e .
```

## Input structure

`<>` means required, `[]` means unused but supported

```
data/
    xxxxx/
        <stock>_<year>_<name>[_*].txt
```