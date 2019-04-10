#! coding: utf-8

# Discovers schemas given CSV files


import csv
import json


def discover(filepath):
    csvfile = open(filepath)
    header = csvfile.readline()[:-1].split(',')

    reader = csv.DictReader(csvfile, header)
    objs = [row for row in reader]

    possible_schema = dict()

    for obj in objs:
        for field in header:
            value = obj[field]

            if field not in possible_schema:
                possible_schema[field] = {
                    'values': set(value),
                    'nullable': value == 'NA'
                }
            else:
                possible_schema[field]['values'].add(value)
                possible_schema[field]['nullable'] = possible_schema[field]['nullable'] or (value == 'NA')

    for field in header:
        print(field)
        print('nullable:', possible_schema[field]['nullable'])
        print('values:', possible_schema[field]['values'])
        print()


if __name__ == '__main__':
    filename = 'comp_boss'
    discover('/home/embat/workspace/Personal/dotz-hiring/storage/raw/{}.csv'.format(filename))
