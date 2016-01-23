from dunagan_utility import sort_float_dictionary_ascending

class SymbolData:
    
    def __init__(self, symbol):
        self.symbol = symbol
        self.today_price = None
        self.deltas = {}
        self.delta_before_value = {}
        self.delta_after_value = {}
        self.spans = {}

    def setTodayPrice(self, today_price):
        self.today_price = today_price
        return self

    def getTodayPrice(self):
        return self.today_price

    def initializeSpanByCode(self, code):
        self.spans[code] = []
        return self

    def addSpanValueByCode(self, code, price):
        self.spans[code].append(price)
        return self

    def getPercentageDeltaOffSpanAverage(self, span_code, price_to_compare):
        span_average = self.getSpanAverageByCode(span_code)
        if span_average is None:
            return None
        delta = price_to_compare - span_average
        delta_percentage = delta / span_average
        return delta_percentage

    def getSpanAverageByCode(self, code):
        span = self.spans[code]
        span_count = len(span)
        if span_count == 0:
            return None
        span_sum = float(sum(span))
        span_average = span_sum / span_count
        return span_average

    def initializeDeltaByCode(self, code):
        self.deltas[code] = 0.0
        self.delta_before_value[code] = False
        self.delta_after_value[code] = False
        return self

    def setDeltaBeforeByCode(self, code, price):
        self.deltas[code] -= price
        self.delta_before_value[code] = price
        return self

    def getDeltaBeforeByCode(self, code):
        return self.delta_before_value[code]

    def setDeltaAfterByCode(self, code, price):
        self.deltas[code] += price
        self.delta_after_value[code] = price
        return self

    def getDeltaAfterByCode(self, code):
        return self.delta_after_value[code]

    def getDeltaByCode(self, code):
        if (not((self.delta_after_value[code] is False) or (self.delta_before_value[code] is False))):
            return self.deltas[code]
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

    def getSortedDeltaValuesByCode(self, delta_code):
        """
        :param string delta_code:
        :return: OrderedDict
        """
        delta_values_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items(): # type: SymbolData
            symbol_delta_value = symbolDataInstance.getDeltaByCode(delta_code)
            if not(symbol_delta_value is None):
                delta_values_by_symbol[symbol] = symbol_delta_value
        sorted_delta_value_symbols_dictionary = sort_float_dictionary_ascending(delta_values_by_symbol)
        return sorted_delta_value_symbols_dictionary

    def getSortedTodayPricePercentageOffSpanAveragesByCode(self, span_code):
        """
        :param string span_code:
        :return: OrderedDict
        """
        span_today_price_off_average_values_by_symbol = {}
        for symbol, symbolDataInstance in self.symbols_dict.items(): # type: SymbolData
            symbol_average_for_span = symbolDataInstance.getSpanAverageByCode(span_code)
            today_price_for_symbol = symbolDataInstance.getTodayPrice()
            if ((symbol_average_for_span is None) or (today_price_for_symbol is None)):
                continue
            today_price_delta_off_average = today_price_for_symbol - symbol_average_for_span
            today_price_percentage_off_delta = today_price_delta_off_average / symbol_average_for_span
            span_today_price_off_average_values_by_symbol[symbol] = today_price_percentage_off_delta
        sorted_span_today_price_off_average_values_by_symbol = sort_float_dictionary_ascending(span_today_price_off_average_values_by_symbol)
        return sorted_span_today_price_off_average_values_by_symbol
