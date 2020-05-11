import csv
from collections import defaultdict
from typing import TextIO, Any, Union
from itertools import groupby


def total_schools(csvfile: TextIO, skiplines: int = 1):
    row_count = 0
    csv_reader = csv.reader(csvfile, delimiter=',')
    for _ in csv_reader:
        if skiplines == 0:
            row_count = row_count + 1
        else:
            skiplines = skiplines - 1
    return row_count


def schools_in_category(csvfile: TextIO, key: Union[str, tuple], skiplines: int = 1, ):
    csv_reader = csv.DictReader(csvfile, delimiter=',')
    # skip the header lines
    while skiplines:
        next(csv_reader)
        skiplines = skiplines - 1
    key_school = defaultdict(int)
    for row in csv_reader:
        if type(key) == tuple:
            category = tuple([row.get(it) for it in key])
        else:
            category = row.get(key)
        key_school[category] = key_school[category] + 1
    return key_school


def print_counts(file_path: str = './school_data.csv', encoding='cp1252'):
    with open(file_path, newline='', encoding=encoding) as csvfile:
        print(f"Total Schools: {total_schools(csvfile)}")

    with open(file_path, newline='', encoding=encoding) as csvfile:
        print("Schools by State:")
        schools_dict = schools_in_category(csvfile, 'LSTATE05')
        for key, school_num in schools_dict.items():
            print(f"{key}: {school_num}")

    with open(file_path, newline='', encoding=encoding) as csvfile:
        print("Schools by Metro-centric locale:")
        schools_dict = schools_in_category(csvfile, 'MLOCALE')
        for key, school_num in schools_dict.items():
            print(f"{key}: {school_num}")

    with open(file_path, newline='', encoding=encoding) as csvfile:
        schools_dict = schools_in_category(csvfile, ('LCITY05', 'LSTATE05'))
        city_schools = max(list(schools_dict.items()), key=lambda item: item[1])
        print(f"City with most schools: {city_schools[0][0]} ({city_schools[1]} schools)")

        # remove all city,state keys that have a common city name
        unique_cities = {city_name: count for city_name, count in list(schools_dict.items()) if count >= 1}
        print(f"Unique cities with at least one school: {len(unique_cities)}")
