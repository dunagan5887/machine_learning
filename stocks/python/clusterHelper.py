class ClusterHelper:

    @staticmethod
    def getKMModelListOfClusterMembers(myKMModel, data_members):
        """

        :param pyspark.mllib.clustering.KMeansModel myKMModel:
        :param dict data_members:
        :return:
        """
        cluster_groups = {}
        for key, vector in data_members.items():
            cluster_number = myKMModel.predict(vector)
            if not(cluster_number in cluster_groups):
                cluster_groups[cluster_number] = {}
            cluster_groups[cluster_number][key] = vector
        return cluster_groups
