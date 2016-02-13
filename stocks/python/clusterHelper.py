from rdd_utility import RddUtility

class ClusterHelper:

    @staticmethod
    def getKMModelDictionaryOfClusterMembersByTuplesRDD(myKMModel, rddOfTuples):
        """
        :param pyspark.mllib.clustering.KMeansModel myKMModel:
        :param RDD data_members:
        :return: RDD
        """
        def predictionForTupleClosure(rdd_tuple):
            """
            :param RDD rdd_tuple:
            :return: tuple (cluster_number, key)
            """
            key = rdd_tuple[0]
            value = rdd_tuple[1]
            cluster_number = myKMModel.predict(value)
            return (cluster_number, key)

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
