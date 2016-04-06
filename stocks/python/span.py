from collections import OrderedDict


class Span:

    def __init__(self, code):
        self.code = code
        self.label_to_unit_index_mapping = {}
        self.units = OrderedDict()

    def getUnitsCount(self):
        return len(self.units)

    def getUnitFieldValuesAsList(self, field):
        list_to_return = []
        for key, spanUnit in self.units.iteritems():
            field_value = getattr(spanUnit, field)
            if not(field_value is None):
                list_to_return.append(field_value)
        return list_to_return

    def addSpanUnit(self, unit_labels_list=None, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        newSpanUnit = SpanUnit(close_price, open_price, high_price, low_price, delta, delta_percentage)
        unit_index = len(self.units)
        if not(isinstance(unit_labels_list, list)):
            unit_labels_list = [unit_labels_list]
        unique_unit_labels_list = set(unit_labels_list)
        self.units[unit_index] = newSpanUnit
        for label in unique_unit_labels_list:
            if not(label in self.label_to_unit_index_mapping):
                self.label_to_unit_index_mapping[label] = list()
            self.label_to_unit_index_mapping[label].append(unit_index)
        return self

    def removeSpanUnit(self, unit_label):
        unit_index = self.label_to_unit_index_mapping[unit_label]


    def getMaxUnitDeltaValue(self, get_percentage = False):
        """
        :param bool get_percentage:
        :return: float
        """
        max_unit_delta = self.getMaxUnitDelta(get_percentage)
        return max_unit_delta['delta']

    def getMaxUnitDelta(self, get_percentage = False):
        """
        :param bool get_percentage:
        :return: dict
        """
        max_unit_delta = float('-inf')
        max_unit_label = None
        max_was_found = False
        for units_label, unit_index_mapping in self.label_to_unit_index_mapping.items():
            span_delta = self.getSpanDelta(units_label, get_percentage)
            if span_delta > max_unit_delta:
                max_unit_label = units_label
                max_unit_delta = span_delta
                max_was_found = True

        if not(max_was_found):
            return {'delta' : None, 'label' : None}

        return {'delta' : max_unit_delta, 'label' : max_unit_label}

    def getMinUnitDeltaValue(self, get_percentage = False):
        """
        :param bool get_percentage:
        :return: float
        """
        min_unit_delta = self.getMinUnitDelta(get_percentage)
        return min_unit_delta['delta']

    def getMinUnitDelta(self, get_percentage = False):
        """
        :param bool get_percentage:
        :return: dict
        """
        min_unit_delta = float('inf')
        min_unit_label = None
        max_was_found = False
        for units_label, unit_index_mapping in self.label_to_unit_index_mapping.items():
            span_delta = self.getSpanDelta(units_label, get_percentage)
            if span_delta < min_unit_delta:
                min_unit_label = units_label
                min_unit_delta = span_delta
                max_was_found = True

        if not(max_was_found):
            return {'delta' : None, 'label' : None}

        return {'delta' : min_unit_delta, 'label' : min_unit_label}

    def getSpanUnitsByLabel(self, units_label):
        """
        :param string units_label:
        :return: OrderedDict
        """
        if not(units_label in self.label_to_unit_index_mapping):
            return None
        dict_of_span_units = OrderedDict()
        for unit_index in self.label_to_unit_index_mapping[units_label]:
            spanUnit = self.units[unit_index]
            dict_of_span_units[unit_index] = spanUnit

        return dict_of_span_units

    def getSpanDelta(self, units_label = None, get_percentage_delta = False):
        if not(units_label is None):
            span_units_for_label = self.getSpanUnitsByLabel(units_label)
            if span_units_for_label is None:
                return None
            units_for_delta = span_units_for_label
        else:
            units_for_delta = self.units

        if len(self.units) == 0:
            return None
        firstUnit = units_for_delta.items()[0][1]
        lastUnit = units_for_delta.items()[-1][1]
        open_price = firstUnit.open_price
        close_price = lastUnit.close_price
        if (not(open_price is None) and not(close_price is None)):
            delta = close_price - open_price
            if not(get_percentage_delta):
                return delta
            if open_price != 0.0:
                return (delta / abs(open_price))
            if close_price == 0.0:
                return 0.0
            return float("inf")
        return None

    def getSpanPriceRangeToAveragePriceRatio(self, units_label = None):
        span_close_average = self.getSpanCloseAverage(units_label)
        if not((span_close_average is None)):
            span_price_range = self.getSpanPriceRange(units_label)
            if not(span_price_range is None):
                ratio = span_price_range / span_close_average
                return ratio

        return None

    def getSpanPriceRange(self, units_label = None):
        if not(units_label is None):
            span_units_for_label = self.getSpanUnitsByLabel(units_label)
            if span_units_for_label is None:
                return None
            units_for_range = span_units_for_label
        else:
            units_for_range = self.units

        max_price = float("-inf")
        min_price = float("+inf")
        non_zero_value_was_found = False
        for index, spanUnit in units_for_range.items():
            unit_high_price = spanUnit.high_price
            unit_low_price = spanUnit.low_price
            if (unit_high_price is None) or (unit_low_price is None):
                continue
            max_price = unit_high_price if unit_high_price > max_price else max_price
            min_price = unit_low_price if unit_low_price < min_price else min_price
            non_zero_value_was_found = True

        if non_zero_value_was_found:
            range = max_price - min_price
            return range
        return None

    def getSpanCloseAverage(self, units_label = None):
        list_of_close_prices = []
        if not(units_label is None):
            span_units_for_label = self.getSpanUnitsByLabel(units_label)
            if span_units_for_label is None:
                return None
            units_to_iterate_over = span_units_for_label.iteritems()
        else:
            units_to_iterate_over = self.units.iteritems()

        for unit_code, SpanUnitObject in units_to_iterate_over:
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
        self.high_price = float(high_price) if not(high_price is None) else None
        self.low_price = float(low_price) if not(low_price is None) else None
        self.open_price = float(open_price) if not(open_price is None) else None
        self.close_price = float(close_price) if not(close_price is None) else None
        self.delta = float(delta) if not(delta is None) else None
        self.delta_percentage = float(delta_percentage) if not(delta_percentage is None) else None