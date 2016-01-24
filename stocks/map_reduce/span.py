from collections import OrderedDict

class Span:

    def __init__(self, code):
        self.code = code
        self.units = OrderedDict()

    def addSpanUnit(self, unit_code, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        newSpanUnit = SpanUnit(unit_code, close_price, open_price, high_price, low_price, delta, delta_percentage)
        self.units[unit_code] = newSpanUnit
        return self

    def getSpanUnitByCode(self, unit_code):
        if unit_code in self.units:
            spanUnit = self.units[unit_code]
            return spanUnit
        return None

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
    def __init__(self, code, close_price = None, open_price = None, high_price = None, low_price = None, delta = None, delta_percentage = None):
        self.code = code
        self.high_price = high_price
        self.low_price = low_price
        self.open_price = open_price
        self.close_price = close_price
        self.delta = delta
        self.delta_percentage = delta_percentage