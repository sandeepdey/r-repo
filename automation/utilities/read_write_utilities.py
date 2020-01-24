from utilities.logging_setup import logger
import csv


def read_list_from_file(filename, isstreaming=False):
    lines = []
    with open(filename, 'r') as reader:
        lines = [x.rstrip() for x in reader.readlines()]
    logger.info("Lines Read From " + filename + " = " + str(len(lines)))
    return lines


# def format_string_towrite():
#
def write_list_to_csv(filename, list_of_dicts):
    keys = list(list_of_dicts[0].keys())
    # print(keys)
    num_lines = 0
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        for row in list_of_dicts:
            dict_writer.writerow(row)
            num_lines += 1
            if num_lines % 1000 == 0: logger.info("%d Lines Written to %s", num_lines, filename)
    logger.info("Total Lines Written to %s = %d ", filename, num_lines)


def write_set_to_txt(filename, list_rows, append=False):
    permissions = 'w' if not append else 'w+'
    with open(filename, permissions) as output_file:
        output_file.writelines(x + '\n' for x in list_rows)
    logger.info("Total Lines Written to %s = %d ", filename, len(list_rows))
