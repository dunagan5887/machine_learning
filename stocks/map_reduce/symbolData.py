class SymbolData:
    
    def __init__(self, symbol):
        self.symbol = symbol
    
class SymbolDataCollection:
    
    def __init__(self):
        self.symbols_dict = {}
    
    def isSymbolInCollection(self, symbol):
        return symbol in self.symbols_dict
    
    def addSymbolToCollection(self, symbol):
        if not(self.isSymbolInCollection(symbol)):
            self.symbols_dict[symbol] = SymbolData(symbol)
            
    def getSymbolData(self, symbol):
        if self.isSymbolInCollection(symbol):
            return self.symbols_dict[symbol]
        return None