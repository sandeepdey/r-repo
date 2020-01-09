import pandas as pd


class Drug:
    mac_fixed_price = 0
    mac_unit_price = 0
    sale_fixed_price = 0
    sale_unit_price = 0

    def __init__(self, row):
        self.top_30ds_quantity = float(row["top_30ds_quantity"])
        self.top_90ds_quantity = float(row["top_90ds_quantity"])
        self.edlp_unit_price = float(row["edlp_unit_price"])
        self.bsd_unit_price = float(row["bsd_unit_price"])
        self.hd_unit_price = float(row["hd_unit_price"])
        self.edlp_fixed_price = float(row["edlp_fixed_price"])
        self.bsd_fixed_price = float(row["bsd_fixed_price"])
        self.hd_fixed_price = float(row["hd_fixed_price"])
        self.bh01_mac_price = float(row["bh01_mac_price"])
        self.bh02_mac_price = float(row["bh02_mac_price"])
        self.bh03_mac_price = float(row["bh03_mac_price"])
        self.wmt_mac_price = float(row["wmt_mac_price"])
        self.hd_syr_mac_price = float(row["hd_syr_mac_price"])
        self.bh01_dispensing_fee = float(row["bh01_dispensing_fee"])
        self.bh02_dispensing_fee = float(row["bh02_dispensing_fee"])
        self.bh03_dispensing_fee = float(row["bh03_dispensing_fee"])
        self.wmt_dispensing_fee = float(row["wmt_dispensing_fee"])
        self.hd_syr_dispensing_fee = float(row["hd_syr_dispensing_fee"])
        self.wmt_2018_11_28_qty1 = float(row["wmt_2018_11_28_qty1"])
        self.wmt_2018_11_28_qty2 = float(row["wmt_2018_11_28_qty2"])
        self.wmt_2018_11_28_price1 = float(row["wmt_2018_11_28_price1"])
        self.wmt_2018_11_28_price2 = float(row["wmt_2018_11_28_price2"])
        self.wmt_2018_11_28_flg = float(row["wmt_2018_11_28_flg"])
        self.last_30ds_qty = float(row["last_30ds_qty"])
        self.last_90ds_qty = float(row["last_90ds_qty"])
        # min_grx_30ds = float(row["min_grx_30ds"])
        # min_grx_90ds = float(row["min_grx_90ds"])
        self.min_major_retail_grx_30ds = float(row["min_major_retail_grx_30ds"])
        self.min_major_retail_grx_90ds = float(row["min_major_retail_grx_90ds"])
        # min_retail_grx_30ds = float(row["min_retail_grx_30ds"])
        # min_retail_grx_90ds = float(row["min_retail_grx_90ds"])
        # ltd_30_day_scripts = float(row["ltd_30_day_scripts"])
        # ltd_90_day_scripts = float(row["ltd_90_day_scripts"])
        # ltd_30_day_scripts_pct = float(row["ltd_30_day_scripts_pct"])
        # ltd_90_day_scripts_pct = float(row["ltd_90_day_scripts_pct"])
        # r30_30_day_scripts = float(row["r30_30_day_scripts"])
        # r30_90_day_scripts = float(row["r30_90_day_scripts"])
        # r30_30_day_script_pct = float(row["r30_30_day_script_pct"])
        # r30_90_day_script_pct = float(row["r30_90_day_script_pct"])
        self.fills = float(row["fills"])
        self.margin = float(row["margin"])
        self.orders = float(row["orders"])
        self.revenue = float(row["revenue"])
        self.users = float(row["users"])
        self.default_quantity = float(row["default_quantity"])

        self.pharmacy_network_id = float(row["pharmacy_network_id"])

        if self.pharmacy_network_id == 1:
            self.mac_unit_price = 0.45 * self.bh01_mac_price + 0.3 * self.wmt_mac_price + 0.175 * self.bh03_mac_price + 0.075 * self.bh02_mac_price
            self.mac_fixed_price = 0.45 * self.bh01_dispensing_fee + 0.3 * self.wmt_dispensing_fee + 0.175 * self.bh03_dispensing_fee + 0.075 * self.bh02_dispensing_fee
            self.sale_fixed_price = self.edlp_fixed_price
            self.sale_unit_price = self.edlp_unit_price
        if self.pharmacy_network_id == 2:
            self.mac_unit_price = self.bh01_mac_price
            self.mac_fixed_price = self.bh01_dispensing_fee
            self.sale_fixed_price = self.bsd_fixed_price
            self.sale_unit_price = self.bsd_unit_price
        if self.pharmacy_network_id == 3:
            self.mac_unit_price = self.hd_syr_mac_price
            self.mac_fixed_price = self.hd_syr_dispensing_fee
            self.sale_fixed_price = self.hd_fixed_price
            self.sale_unit_price = self.hd_unit_price

        # For Default Quantity
        self.sale_price_30ds_qty = self.sale_unit_price * self.last_30ds_qty + self.sale_fixed_price
        self.mac_price_30ds_qty = self.mac_unit_price * self.last_30ds_qty + self.mac_fixed_price

        self.sale_price_wmt_qty1 = self.sale_unit_price * self.wmt_2018_11_28_qty1 + self.sale_fixed_price
        self.sale_price_wmt_qty2 = self.sale_unit_price * self.wmt_2018_11_28_qty2 + self.sale_fixed_price
        self.mac_price_wmt_qty1 = self.mac_unit_price * self.wmt_2018_11_28_qty1 + self.mac_fixed_price
        self.mac_price_wmt_qty2 = self.mac_unit_price * self.wmt_2018_11_28_qty2 + self.mac_fixed_price

        self.inConsideration = row['in_consideration']
        self.isMarginPositive = self.mac_price_30ds_qty < self.sale_price_30ds_qty
        self.isCompetitive = self.sale_price_30ds_qty < self.min_major_retail_grx_30ds if pd.notna(
            self.last_30ds_qty) else False
        self.walmartDrugGroup = True if pd.notna(self.wmt_2018_11_28_flg) else False

        self.pricesChanged = False
        self.newer_unit_price = self.sale_unit_price
        self.newer_fixed_price = self.sale_fixed_price
        self.comment = 'No Change'

    def getCurrentPrice(self, quantity):
        return self.sale_unit_price * quantity + self.sale_fixed_price

    def getMac(self, quantity):
        return self.mac_unit_price * quantity + self.mac_fixed_price

    def setUnitFixedPrice(self, qty1, price1, qty2, price2, comment):
        if (price1 < price2) and (qty1 < qty2):
            self.pricesChanged = True
            self.newer_unit_price = (price2 - price1) / (qty2 - qty1)
            self.newer_fixed_price = price1 - self.newer_unit_price * qty1
            self.comment = comment
        else:
            self.pricesChanged = False
            comment = comment + '; quantities not defined clearly'

    def setToMacPrice(self, comment='Setting to Mac Prices'):
        self.pricesChanged = True
        self.newer_unit_price = self.mac_unit_price
        self.newer_fixed_price = self.mac_fixed_price
        self.comment = comment

    def getNewPrices(self, quantity):
        return self.newer_unit_price * quantity + self.newer_fixed_price

    def getChanges(self):
        return self.newer_unit_price, self.newer_fixed_price, self.getCurrentPrice(
            self.default_quantity), self.getNewPrices(self.default_quantity), self.comment

    def setFinalPrices(self):
        if self.inConsideration and float(self.margin) < -300:
            if self.walmartDrugGroup:
                if self.sale_price_wmt_qty1 < self.wmt_2018_11_28_price1:
                    self.setUnitFixedPrice(self.wmt_2018_11_28_qty1, self.wmt_2018_11_28_price1,
                                           self.wmt_2018_11_28_qty2, self.wmt_2018_11_28_price2,
                                           "WMT 4/10|9/24 Drug; Raising to WMT Prices")
                elif self.wmt_2018_11_28_price1 > self.mac_price_wmt_qty1 - 5:
                    self.setUnitFixedPrice(self.wmt_2018_11_28_qty1, self.wmt_2018_11_28_price1,
                                           self.wmt_2018_11_28_qty2, self.wmt_2018_11_28_price2,
                                           "WMT 4/10|9/24 Drug; Dropping to WMT Prices")
                else:
                    self.setUnitFixedPrice(self.wmt_2018_11_28_qty1, self.mac_price_wmt_qty1 - 5,
                                      self.wmt_2018_11_28_qty2, self.mac_price_wmt_qty2 - 5,
                                      "WMT 4/10|9/24 Drug; Dropping to MAC - 5 , MAC - WMT > 5")
            elif not self.isMarginPositive:
                self.setToMacPrice(comment='NON-WMT Drug; Margin -VE; Raising to MAC Prices')
            # else:
                # Is Margin +ve , Dont Change
                # append(noChange=True, comment='NON-WMT Drug; Margin +VE; Keeping the Same Prices')
                # if not isCompetitive:
                #
                #     # set it to competitive levels
                #     pass
                # else :
