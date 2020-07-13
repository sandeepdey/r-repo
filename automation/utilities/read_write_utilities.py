from utilities.logging_setup import logger
import csv
import os.path
from os import path
import pprint

def read_set(filename, isstreaming=False):
    lines = []
    with open(filename, 'r') as reader:
        lines = [x.rstrip() for x in reader.readlines()]
    logger.info("Lines Read From " + filename + " = " + str(len(lines)))
    return lines

def write_set(filename, list_rows, append=False):
    permissions = 'w' if not append else 'w+'
    with open(filename, permissions) as output_file:
        output_file.writelines(x + '\n' for x in list_rows)
    logger.info("Total Lines Written to %s = %d ", filename, len(list_rows))

def read_csv(filename):
    if not path.exists(filename):
        return None
    with open(filename) as csvfile:
        dictreader = csv.DictReader(csvfile)
        return [row for row in dictreader]

def write_to_csv(filename, data, append=False , delimiter = ',' , ignoreFieldErrors=True , header=None): #data is assumed to be list of dicts
    if data is None or len(data) == 0 or type(data) is not list or type(data[0]) is not dict:
        logger.error('Data Not Correct')
        return

    if not path.exists(filename):
        append = False
    permissions = 'w' if not append else 'w+'

    if header is None:
        header = list(data[0].keys())

    if append : #get header
        with open(filename) as headerfile:
            header = headerfile.readline().split(delimiter)

    logger.info('Header for output : %s'%header)

    num_lines = 0
    with open(filename, permissions) as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=header)
        if not append:
            dict_writer.writeheader()
        header_set = frozenset(header)
        for row in data:
            row_headers = frozenset(row.keys())
            if ignoreFieldErrors:
                for fieldName in header_set-row_headers:
                    row[fieldName] = ''
                for fieldName in row_headers - header_set:
                    del row[fieldName]
            dict_writer.writerow(row)
            num_lines += 1
            if num_lines % 1000 == 0: logger.info("%d Lines Written to %s", num_lines, filename)
    logger.info("Total Lines Written to %s = %d ", filename, num_lines)

pp = pprint.PrettyPrinter(indent=4)
def print_object(obj):
    pp.pprint(obj)