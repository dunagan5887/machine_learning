#!/usr/bin/env python

from symbolData import SymbolData
from symbolData import SymbolDataCollection

newCollection = SymbolDataCollection()

test_symbol = 'A'

is_symbol_in_collection = newCollection.isSymbolInCollection(test_symbol)

if is_symbol_in_collection != False:
    raise ValueError('Collection did not return False for a symbol not yet in the collection')
    
other_symbol = 'B'
newCollection.addSymbolToCollection(test_symbol)
is_other_symbol_in_collection = newCollection.isSymbolInCollection(other_symbol)

if is_other_symbol_in_collection != False:
    raise ValueError('Collection did not return False for a symbol not yet in the collection after a different symbol had been added')
    
testSymbolData = newCollection.getSymbolData(test_symbol)

if type(testSymbolData) is SymbolData:
    raise ValueError('An object of type SymbolData was not returned for a symbol which should be in the SymbolDataCollection')
    
test_symbol_from_collection = testSymbolData.symbol

if test_symbol_from_collection != test_symbol:
    raise ValueError('The symbol from a SymbolData object returned from a SymbolDataCollection did not match the symbol passed into newCollection.getSymbolData()')
    
newCollection.addSymbolToCollection(other_symbol)
otherSymbolData = newCollection.getSymbolData(other_symbol)
other_symbol_from_data = otherSymbolData.symbol

if other_symbol_from_data != other_symbol:
    raise ValueError('The symbol from another SymbolData object returned from a SymbolDataCollection did not match the other symbol passed into newCollection.getSymbolData()')

