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

span_to_test = [1.34, 5.6, -3.5, -0.3, 2.2]
span_expected_average = 1.068
span_code = 'test_span'
testSymbolData.initializeSpanByCode(span_code)
for value in span_to_test:
    testSymbolData.addSpanValueByCode(span_code, value)
span_test_average = testSymbolData.getSpanAverageByCode(span_code)
if span_expected_average != span_test_average:
    raise ValueError('The computed average for a test span for a test SymbolData object was expected to be {0} but was {1}'.format(span_expected_average, span_test_average))
off_average_value_to_test = 1.12
expected_delta_off_average = 0.0486891385768
test_delta_off_average = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test)
test_delta_off_average = round(test_delta_off_average, 13)
if expected_delta_off_average != test_delta_off_average:
    raise ValueError('The computed delta percentage off average for a test SymbolData object was expected to be {0} but was {1}'.format(expected_delta_off_average, test_delta_off_average))
before_delta_test = 4.5
after_delta_test = 3.2
expected_delta = -1.3
delta_code = 'test_delta'
testSymbolData.initializeDeltaByCode(delta_code)
testSymbolData.setDeltaBefore(delta_code, before_delta_test)
testSymbolData.setDeltaAfter(delta_code, after_delta_test)
test_delta = testSymbolData.getDeltaByCode(delta_code)
test_delta = round(test_delta,1)
if test_delta != expected_delta:
    raise ValueError('The computed delta for a test SymbolData object was expected to be {0} but was {1}'.format(expected_delta, test_delta))



