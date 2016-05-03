from rdd_utility import RddUtility

class ClusterHelper:

    @staticmethod
    def getKMModelDictionaryOfClusterMembersByTuplesRDD(myKMModel, rddOfTuples):
        """
        :param pyspark.mllib.clustering.KMeansModel myKMModel: This is the KMeans clustering model which was generated
                                                                via the call to KMeans.train(). We will feed each stock's
                                                                set of data points into the training model to determine
                                                                which cluster each stock belongs to

        :param RDD rddOfTuples: This is an RDD of tuples of the form (symbol, data_point_tuples). This RDD should contain
                                    the tuples for all stocks which were clustered. We will be feeding each of these stock's
                                    data points into the clustering model to determine which cluster each stock belongs to

        :return: RDD: Will return a dictionary mapping cluster_numbers to a list of symbols which belong to that cluster
                        E.g. will be of the form {1: ['FB', 'AMZN'], 2: ['SBUX', 'MCD'], 3: ['GOOG', 'NFLX']}
        """
        def predictionForTupleClosure(rdd_tuple):
            """
                This is a function closure which will determine which cluster a given stock belongs to in the myKMModel

            :param tuple rdd_tuple: A tuple of the form (symbol, data_point_tuples_list). Each element in each
                                        data_point_tuples_list is of the form (data_point_description, data_point_value)

            :return: tuple (cluster_number, stock_symbol)
            """
            symbol = rdd_tuple[0]
            list_of_tuples = rdd_tuple[1]

            # Get the list of data_points_values which should be fed into the clustering model
            list_of_values_to_predict = map(lambda symbol_data_tuple : symbol_data_tuple[1], list_of_tuples)

            # Get the cluster number for the list of values
            cluster_number = myKMModel.predict(list_of_values_to_predict)

            # Return a tuple mapping the symbol to the cluster number
            return (cluster_number, symbol)

        """

        The dictionary mapping cluster numbers to the respective list of member symbols is produced via the steps below:
            .map(predictionForTupleClosure) - Feed each symbol's data into the KMeans clustering model to determine which
                                                cluster each belongs to
            .map(lambda tuple : (tuple[0], [tuple[1]])) - Convert each tuple of the form (cluster_number, symbol) into a
                                                            tuple of the form (cluster_number, [symbol]) in order to allow
                                                            us to aggregate lists of symbols in the next step
            .reduceByKey(lambda a,b : a + b) - Using the symbols as keys, reduce the RDD. This will aggregate tuples which
                                                belong to the same cluster number. For example, (1, ['FB']) and (1, ['AMZN'])
                                                would be reduced to (1, ['FB', 'AMZN'])
            .map(lambda tuple : {tuple[0] : tuple[1]}) - Convert tuples of the form (1, ['FB', 'AMZN']) into one-element
                                                            dictionaries of the form {1: ['FB', 'AMZN']}
            .reduce(RddUtility.combineDictionaries) - Combine all dictionaries together. For example, convert
                                                        {1: ['FB', 'AMZN']} and {2: ['SBUX', 'MCD']} into a single dictionary
                                                        {1: ['FB', 'AMZN'], 2: ['SBUX', 'MCD']}

        """
        clusterNumberToKeyDict = rddOfTuples.map(predictionForTupleClosure)\
                                            .map(lambda tuple : (tuple[0], [tuple[1]]))\
                                            .reduceByKey(lambda a,b : a + b)\
                                            .map(lambda tuple : {tuple[0] : tuple[1]})\
                                            .reduce(RddUtility.combineDictionaries)

        return clusterNumberToKeyDict

    @staticmethod
    def getKMModelListOfClusterMembersByDict(myKMModel, data_members_dict):
        """
        :param pyspark.mllib.clustering.KMeansModel myKMModel:
        :param dict data_members:
        :return:
        """
        cluster_groups = {}
        for key, vector in data_members_dict.items():
            cluster_number = myKMModel.predict(vector)
            if not(cluster_number in cluster_groups):
                cluster_groups[cluster_number] = {}
            cluster_groups[cluster_number][key] = vector
        return cluster_groups

    @staticmethod
    def getKMModelListOfClusterMembersAndValuesByValueAndKeyVectors(myKMModel, values_vector, keys_vector):
        """
        :param pyspark.mllib.clustering.KMeansModel myKMModel:
        :param list values_vector:
        :param list keys_vector:
        :return:
        """
        cluster_groups = {}
        index = 0
        for key in keys_vector:
            value_vector =  values_vector[index]
            cluster_number = myKMModel.predict(value_vector)
            if not(cluster_number in cluster_groups):
                cluster_groups[cluster_number] = {}
            cluster_groups[cluster_number][key] = value_vector
            index += 1
        return cluster_groups

    @staticmethod
    def getKMModelListOfClusterMembersByValueAndKeyVectors(myKMModel, values_vector, keys_vector):
        """
        :param pyspark.mllib.clustering.KMeansModel myKMModel:
        :param list values_vector:
        :param list keys_vector:
        :return:
        """
        cluster_groups = {}
        index = 0
        for key in keys_vector:
            value_vector =  values_vector[index]
            cluster_number = myKMModel.predict(value_vector)
            if not(cluster_number in cluster_groups):
                cluster_groups[cluster_number] = []
            cluster_groups[cluster_number].append(key)
            index += 1
        return cluster_groups
