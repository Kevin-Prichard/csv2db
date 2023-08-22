#!/usr/bin/env python3

from sqlite3 import connect


con = connect("/home/kev/projs/quicknoot/db/FoodData_Central_foundation_food_csv_2023-04-20C.db")
rowcount = dict()
import pudb; pu.db
for name in con.execute("select name from sqlite_master"):
    _name = name[0]
    rowcount[_name] = con.execute(f'select count(*) from {_name}').fetchone()[0]

for name, cnt in rowcount.items():
    print(f"{name}\t{cnt}")



