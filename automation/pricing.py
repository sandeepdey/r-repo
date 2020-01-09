from utilities.redshift_api import RedshiftQueryRunner
import pandas as pd
import datetime
from Drug_Class_Info import Drug

output_unit_price = []
output_fixed_price = []
output_current_def_qty_price = []
output_new_def_qty_price = []
output_comment = []


def loadData():
    # script = "sql/collect_data_about_gcns.sql"
    rqr = RedshiftQueryRunner()
    # data = rqr.get_data_frame_from_sql(script)
    data = rqr.get_data_frame_from_query('select * from mktg_dev.sdey_collect_data_about_gcns ')
    return data
    # data.to_csv('data/gcn_data.csv')
    # panda_data = pd.read_csv('data/gcn_data.csv')
    # return panda_data


def dumpPricingData(data):
    data['output_unit_price'] = output_unit_price
    data['output_fixed_price'] = output_fixed_price
    data['output_current_def_qty_price'] = output_current_def_qty_price
    data['output_new_def_qty_price'] = output_new_def_qty_price
    data['output_comment'] = output_comment
    current_date = str(datetime.datetime.now().date())
    filename = 'data/pricing_final_data_%s.csv' % current_date
    data.to_csv(filename)


def append(drug):
    newer_unit_price, newer_fixed_price, currentPrice_default, newPrice_default, comment = drug.getChanges()
    output_unit_price.append(newer_unit_price)
    output_fixed_price.append(newer_fixed_price)
    output_current_def_qty_price.append(currentPrice_default)
    output_new_def_qty_price.append(newPrice_default)
    output_comment.append(comment)

def parse_drugs(drug_data):
    for index, row in drug_data.iterrows():
        drug = Drug(row)
        drug.setFinalPrices()
        append(drug)


def main():
    data = loadData()
    parse_drugs(data)
    # dumpPricingData(data)


if __name__ == "__main__":
    main()
