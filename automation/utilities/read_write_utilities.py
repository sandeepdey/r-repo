from utilities.logging_setup import logger

def read_list_from_file(filename,isstreaming=False):
    lines = []
    with open(filename,'r') as reader:
        lines = [x.rstrip() for x in reader.readlines()]
    logger.info("Lines Read From " + filename + " = " + str(len(lines)))
    return lines

# def format_string_towrite():
#
# def write_list_to_csv(filename,list_of_dicts,separator=',',header_string=''):
#     header = []
#     if (header_string!='') :
#         header = header_string.split(separator)
#     else:
#         header = list_of_dicts[0].keys()
#
#
#     with
#