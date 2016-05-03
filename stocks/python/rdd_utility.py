
class RddUtility:

    @staticmethod
    def reduceKeyValuePairRddToKeyListRdd(keyValuePairRdd):
        return keyValuePairRdd.map(lambda tuple : (tuple[0], [tuple[1]])).reduceByKey(lambda a,b : a + b)

    @staticmethod
    def combineDictionaries(dict_a, dict_b):
        """
            This function will take 2 dictionaries and combine their elements to form a single dictionary

        :param dict dict_a:
        :param dict dict_b:
        :return: dict
        """
        result_dict = dict_a.copy()
        result_dict.update(dict_b)
        return result_dict

    @staticmethod
    def reduceKeyValueRddToDictionary(keyValuePairRdd):
        return keyValuePairRdd.map(lambda tuple : {tuple[0] : tuple[1]}).reduce(RddUtility.combineDictionaries)


