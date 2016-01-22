class SymbolData:
    
    def __init__(self, symbol):
        self.symbol = symbol
        self.deltas = {}
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
        delta = price_to_compare - span_average
        delta_percentage = delta / span_average
        return delta_percentage

    def getSpanAverageByCode(self, code):
        span = self.spans[code]
        span_count = len(span)
        span_sum = float(sum(span))
        span_average = span_sum / span_count
        return span_average

    def initializeDeltaByCode(self, code):
        self.deltas[code] = 0.0
        return self

    def setDeltaBefore(self, code, price):
        self.deltas[code] -= price
        return self

    def setDeltaAfter(self, code, price):
        self.deltas[code] += price
        return self

    def getDeltaByCode(self, code):
        return self.deltas[code]
    
class SymbolDataCollection:
    
    def __init__(self):
        self.symbols_dict = {}
    
    def isSymbolInCollection(self, symbol):
        return symbol in self.symbols_dict
    
    def addSymbolToCollection(self, symbol):
        if not(self.isSymbolInCollection(symbol)):
            self.symbols_dict[symbol] = SymbolData(symbol)


    def getSymbolData(self, symbol):
        """ :rtype: SymbolData|None """
        if self.isSymbolInCollection(symbol):
            return self.symbols_dict[symbol]
        return None