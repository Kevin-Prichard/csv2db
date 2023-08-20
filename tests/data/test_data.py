zip_filename = 'tests/data/zips/main_and_two_subdir_csv.zip'
sources = [
    "data/csv/main_and_two_subdir_csv/subdir2/microbe.csv",
    "data/csv/main_and_two_subdir_csv/food_protein_conversion_factor.csv",
    "data/csv/main_and_two_subdir_csv/subdir1/measure_unit.csv",
]
structures = {
    "food_protein_conversion_factor": [
        ("food_nutrient_conversion_factor_id", "int", None),
        ("value", "int", None)
    ],
    "measure_unit": [
        ('id', 'int', None),
        ('name', 'str', 16)
    ],
    "microbe": [
        ('id', 'int', None),
        ('foodId', 'int', None),
        ('method', 'str', 19),
        ('microbe_code', 'str', 28),
        ('min_value', 'int', None),
        ('max_value', 'str', 0),
        ('uom', 'str', 5),
    ]
}
