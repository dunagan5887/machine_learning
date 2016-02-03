from dunagan_utility import sort_float_dictionary_ascending
from collections import OrderedDict
from span import Span

class SymbolData:
    
    def __init__(self, symbol):
        self.symbol = symbol
        self.today_price = None
        self.spans = {}

    def setTodayPrice(self, today_price):
        self.today_price = today_price
        return self

    def getTodayPrice(self):
        return self.today_price

    def initializeSpanByCode(self, span_code):
        self.spans[span_code] = Span(span_code)
        return self

    def addSpanValueByCode(self, span_code, unit_labels, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        self.spans[span_code].addSpanUnit(unit_labels, close_price, open_price, high_price, low_price, delta, delta_percentage)
        return self

    def getPercentageDeltaOffSpanAverage(self, span_code, price_to_compare, units_code = None):
        span_average = self.getSpanAverageByCode(span_code, units_code)
        if span_average is None:
            return None
        delta = price_to_compare - span_average
        delta_percentage = delta / span_average
        return delta_percentage

    def getSpanAverageByCode(self, span_code, units_code = None):
        span = self.spans[span_code]
        return span.getSpanCloseAverage(units_code)

    def getSpanDeltaByCode(self, span_code, units_code = None, get_percentage = False):
        span = self.spans[span_code]
        return span.getSpanDelta(units_code, get_percentage)

    def getMaxDeltaForSpanValue(self, span_code, get_percentage = False):
        """
        :param string span_code:
        :param bool get_percentage:
        :return: float
        """
        span = self.spans[span_code]
        max_delta = span.getMaxUnitDeltaValue(get_percentage)
        return max_delta

    def getMaxDeltaForSpan(self, span_code, get_percentage = False):
        """
        :param string span_code:
        :param bool get_percentage:
        :return: dict
        """
        span = self.spans[span_code]
        max_delta = span.getMaxUnitDelta(get_percentage)
        return max_delta

    def getMinDeltaForSpanValue(self, span_code, get_percentage = False):
        """
        :param string span_code:
        :param bool get_percentage:
        :return: float
        """
        span = self.spans[span_code]
        min_delta = span.getMinUnitDeltaValue(get_percentage)
        return min_delta

    def getMinDeltaForSpan(self, span_code, get_percentage = False):
        """
        :param string span_code:
        :param bool get_percentage:
        :return: dict
        """
        span = self.spans[span_code]
        min_delta = span.getMinUnitDelta(get_percentage)
        return min_delta

    def getSpanDeltaValueToSpanMaxDeltaRatio(self, span_delta_code, span_max_delta_code, get_percentage = False):
        """
        :param string span_delta_code:
        :param string span_max_delta_code:
        :return: float|None
        """
        span_delta_value = self.getSpanDeltaByCode(span_delta_code, get_percentage = get_percentage)
        span_max_delta_value = self.getMaxDeltaForSpanValue(span_max_delta_code, get_percentage = get_percentage)
        if (not(span_delta_value is None) and not(span_max_delta_value is None)):
            return float(span_delta_value) / span_max_delta_value
        return None

    def getSpanDeltaValueToSpanMinDeltaRatio(self, span_delta_code, span_min_delta_code, get_percentage = False):
        """
        :param string span_delta_code:
        :param string span_min_delta_code:
        :return: float|None
        """
        span_delta_value = self.getSpanDeltaByCode(span_delta_code, get_percentage = get_percentage)
        span_min_delta_value = self.getMinDeltaForSpanValue(span_min_delta_code, get_percentage = get_percentage)
        if (not(span_delta_value is None) and not(span_min_delta_value is None)):
            return float(span_delta_value) / span_min_delta_value
        return None

class SymbolDataCollection:
    
    def __init__(self):
        self.symbols_dict = {}
    
    def isSymbolInCollection(self, symbol):
        return symbol in self.symbols_dict
    
    def addSymbolToCollection(self, symbol):
        """
        :param string symbol:
        :return: SymbolData
        """
        if not(self.isSymbolInCollection(symbol)):
            self.symbols_dict[symbol] = SymbolData(symbol)
            return self.symbols_dict[symbol]
        else:
            return self.getSymbolData(symbol)


    def getSymbolData(self, symbol):
        """ :rtype: SymbolData|None """
        if self.isSymbolInCollection(symbol):
            return self.symbols_dict[symbol]
        return None

    def getSortedSpanDeltaValueToSpanMaxDeltaRatios(self, span_delta_code, span_max_delta_code, get_percentage = False):
        """
        :param string span_delta_code:
        :param string span_max_delta_code:
        :param bool get_percentage:
        :return: OrderedDict
        """
        span_delta_ratios_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items():  # type: SymbolData
            span_max_delta_ratio = symbolDataInstance.getSpanDeltaValueToSpanMaxDeltaRatio(span_delta_code, span_max_delta_code, get_percentage)
            if not(span_max_delta_ratio is None):
                span_delta_ratios_by_symbol[symbol] = span_max_delta_ratio
        sorted_span_delta_ratios_by_symbol = sort_float_dictionary_ascending(span_delta_ratios_by_symbol)
        return sorted_span_delta_ratios_by_symbol

    def getSortedSpanDeltaValueToSpanMinDeltaRatios(self, span_delta_code, span_min_delta_code, get_percentage = False):
        """
        :param string span_delta_code:
        :param string span_min_delta_code:
        :param bool get_percentage:
        :return: OrderedDict
        """
        span_delta_ratios_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items():  # type: SymbolData
            span_min_delta_ratio = symbolDataInstance.getSpanDeltaValueToSpanMinDeltaRatio(span_delta_code, span_min_delta_code, get_percentage)
            if not(span_min_delta_ratio is None):
                span_delta_ratios_by_symbol[symbol] = span_min_delta_ratio
        sorted_span_delta_ratios_by_symbol = sort_float_dictionary_ascending(span_delta_ratios_by_symbol)
        return sorted_span_delta_ratios_by_symbol

    def getSortedSpanDeltaValuesByCode(self, span_code, unit_code = None, get_percentages = False):
        """
        :param string delta_code:
        :param string|None unit_code:
        :param bool get_percentages:
        :return: OrderedDict
        """
        delta_values_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items():  # type: SymbolData
            symbol_span_unit_delta_value = symbolDataInstance.getSpanDeltaByCode(span_code, unit_code, get_percentages)
            if not(symbol_span_unit_delta_value is None):
                delta_values_by_symbol[symbol] = symbol_span_unit_delta_value
        sorted_delta_value_symbols_dictionary = sort_float_dictionary_ascending(delta_values_by_symbol)
        return sorted_delta_value_symbols_dictionary

    def getSortedTodayPricePercentageOffSpanAveragesByCode(self, span_code, units_label = None):
        """
        :param string span_code:
        :param string|None units_label:
        :return: OrderedDict
        """
        span_today_price_off_average_values_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items(): # type: SymbolData
            symbol_average_for_span = symbolDataInstance.getSpanAverageByCode(span_code, units_label)
            today_price_for_symbol = symbolDataInstance.getTodayPrice()
            if ((symbol_average_for_span is None) or (today_price_for_symbol is None)):
                continue
            today_price_delta_off_average = today_price_for_symbol - symbol_average_for_span
            today_price_percentage_off_delta = today_price_delta_off_average / symbol_average_for_span
            span_today_price_off_average_values_by_symbol[symbol] = today_price_percentage_off_delta
        sorted_span_today_price_off_average_values_by_symbol = sort_float_dictionary_ascending(span_today_price_off_average_values_by_symbol)
        return sorted_span_today_price_off_average_values_by_symbol

    def getSortedDictionaryOfValuesAboveTodayPriceThreshold(self, sorted_dictionary, price_threshold):
        sorted_dictionary_above_threshold = OrderedDict()
        for symbol, symbol_value in sorted_dictionary.items():
            symbolDataInstance = self.getSymbolData(symbol)
            if not(symbolDataInstance is None):
                symbol_today_price = symbolDataInstance.getTodayPrice()
                if (not(symbol_today_price is None) and (symbol_today_price > price_threshold)):
                    sorted_dictionary_above_threshold[symbol] = symbol_value
        return sorted_dictionary_above_threshold
