# csv2db - Convert delimited data files to database tables

csv2db is a tool for generating database table schemas from delimited data files.

It was originally developed to support the [USDA FoodData Central project](https://fdc.nal.usda.gov/), but it can be used for just about any source of delimited data that can be parsed by Python's `csv.reader` class.

There are many "csv2(sql|db|...)" projects, but this one is mine.

## Features

* specify a ZIP file containing CSV files and path to a SQLite database file
* Scans all CSV files
* obtains field names from first row of each CSV file
* uses statistical inference to determine the most inclusive column type (defaults to VARCHAR when no clear winner is found)
* optionally imports from data source to the created database

## Installation
1. Clone repo
2. Create a virtual environment.  For example, `venv`-
```bash
% python -m venv .venv
% source .venv/bin/activate
% export PATH=${PATH}:${PWD}
```

## Usage
```bash
% ./csv2db.py --help
usage: csv2db.py [-h] [--file FILE] [--dir OUTPUT_DIR] [--limit [MAX_CSV_FILES]] [--max [MAX_CSV_ROWS]] [--sqlite SQLITE_DB_FILE]

CSV Schema Generator:extracts the top -n rows from .CSV files in a .ZIP archive.

optional arguments:
  -h, --help            show this help message and exit
  --file FILE, -f FILE
  --dir OUTPUT_DIR, -d OUTPUT_DIR
  --limit [MAX_CSV_FILES], -l [MAX_CSV_FILES]
  --max [MAX_CSV_ROWS], -n [MAX_CSV_ROWS]
  --sqlite SQLITE_DB_FILE, -s SQLITE_DB_FILE
```

### Examples

```bash
% csv2db.py --file my_csv_archive.zip --sqlite my_database.sqlite
```
