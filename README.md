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
usage: ./csv2db.py [-h] [--zip ZIP_FILE] [--sqlite SQLITE_DB_FILE] [--filter NAME_FILTER] [--create-only] [--show-struct] [--save-struct SAVE_STRUCT] [--max [MAX_CSV_ROWS]] [--progress-rows EVERY_ROWS]
                   [--progress-pct EVERY_PCT]

CSV Schema Generator:extracts the top -n rows from .CSV files in a .ZIP archive.

optional arguments:
  -h, --help            show this help message and exit
  --zip ZIP_FILE, -z ZIP_FILE
  --sqlite SQLITE_DB_FILE, -s SQLITE_DB_FILE
  --filter NAME_FILTER, -f NAME_FILTER
  --create-only, -c     Create tables in target db, then exit
  --show-struct, -w     Show scan statistics & table structure then exit
  --save-struct SAVE_STRUCT, -t SAVE_STRUCT
                        Filename to write scan statistics & table structure, then exit
  --max [MAX_CSV_ROWS], -n [MAX_CSV_ROWS]
  --progress-rows EVERY_ROWS, -r EVERY_ROWS
                        Show progress update every -r rows
  --progress-pct EVERY_PCT, -p EVERY_PCT
                        Show progress update every -p percent of a csv file
```

### Examples

```bash
% csv2db.py --file my_csv_archive.zip --sqlite my_database.sqlite
```
