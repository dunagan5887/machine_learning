from collections import OrderedDict

class Span:

    def __init__(self, code):
        self.code = code
        self.label_to_unit_index_mapping = {}
        self.units = OrderedDict()

    def addSpanUnit(self, unit_labels_list=None, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        newSpanUnit = SpanUnit(close_price, open_price, high_price, low_price, delta, delta_percentage)
        unit_index = len(self.units)
        self.units[unit_index] = newSpanUnit
        for label in unit_labels_list:
            if not(label in self.label_to_unit_index_mapping):
                self.label_to_unit_index_mapping[label] = list()
            self.label_to_unit_index_mapping[label].append(unit_index)
        return self

    def getSpanUnitsByLabel(self, units_label):
        """
        :param string units_label:
        :return: dict
        """
        if not(units_label in self.label_to_unit_index_mapping):
            return None
        dict_of_span_units = {}
        for unit_index in self.label_to_unit_index_mapping[units_label]:
            spanUnit = self.units[unit_index]
            dict_of_span_units[unit_index] = spanUnit

        return dict_of_span_units

    def getSpanDelta(self):
        if len(self.units) == 0:
            return None
        firstUnit = self.units.items()[0][1]
        lastUnit = self.units.items()[-1][1]
        open_price = firstUnit.open_price
        close_price = lastUnit.close_price
        if (not(open_price is None) and not(close_price is None)):
            return close_price - open_price
        return None

    def getSpanCloseAverage(self):
        list_of_close_prices = []
        for unit_code, SpanUnitObject in self.units.iteritems():
            close_price = SpanUnitObject.close_price
            if not(close_price is None):
                list_of_close_prices.append(close_price)
        close_price_length = len(list_of_close_prices)
        if close_price_length == 0:
            return None
        close_price_sum = float(sum(list_of_close_prices))
        close_price_average = close_price_sum / close_price_length
        return close_price_average

class SpanUnit:
    def __init__(self, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        self.high_price = high_price
        self.low_price = low_price
        self.open_price = open_price
        self.close_price = close_price
        self.delta = delta
        self.delta_percentage = delta_percentage