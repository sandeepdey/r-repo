import whoosh
from utilities.read_write_utilities import read_csv
from whoosh.index import create_in,open_dir
from whoosh.fields import *



def create_index():
    schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
    ix = create_in("/Users/sandeep.dey/Documents/data/indexdir", schema)
    title_fields = ['generic_name_long','strength']
    content_fields = ['routed_generic_desc','strength','dosage_form_desc','generic_name_long']
    writer = ix.writer()
    data = read_csv('/Users/sandeep.dey/Documents/data/indexingdata.cv.csv')
    for record in data:
        title = ' '.join([record[field] for field in title_fields]).lower()
        id = record['gcn']
        content = ' '.join([record[field] for field in content_fields]).lower()
        writer.add_document(title=title, path=id,content=content)
    writer.commit()

# ix = open_dir("/Users/sandeep.dey/Documents/data/indexdir")
#
# #
# #
#
#
# searcher = ix.searcher()
