
class ETL(object):
    """
    Abstract class to provide scaffolding for ETL subclasses. Basically just outlines order
    of operations. All application-specific logic should be written into subclass methods.

    Whole process can be accessed via the `ETL.run` method
    """
    def __init__(self, query_options=None):
        self.query_options = query_options

    def run_query(self, **kwargs):
        """
        Runs a query and returns a result

        :param kwargs: passed to template
        :return:
        """
        # DO SOME QUERYING HERE, return response object of some sort
        return {}

    def extract(self, **kwargs):
        """
        Runs query and loads response into a Dataframe

        :param kwargs: all kwargs passed directly into template as template vars
        :return:
        """
        query_response = self.run_query(**kwargs)
        return query_response

    def transform(self, dataframe):
        """
        Hook for subclasses to do any necessary transformation
        of raw query output before inserting into MongoDB.

        Base class just passes dataframe right through.

        :param dataframe:
        :return:
        """
        return dataframe

    def load(self, dataframe):
        """
        Hook for subclasses to load data into external datastore.

        Base class just passes dataframe right through.
        :param dataframe:
        :return:
        """
        return dataframe

    def run(self, **kwargs):
        dataframe = self.extract(**kwargs)
        if dataframe is not None:
            dataframe = self.transform(dataframe)
            result = self.load(dataframe)
            return result
        else:
            return None