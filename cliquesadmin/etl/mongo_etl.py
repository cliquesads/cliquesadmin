import logging
import pandas as pd
from cliquesadmin.etl import ETL

logger = logging.getLogger(__name__)


class MongoAggregationETL(ETL):
    """
    Runs an aggregation against a MongoDB and inserts results to another collection.

    - Runs aggregation against input_mongo_collection collection using a pipeline generator
        function which returns any valid MongoDB pipeline
    - Stores query result into Dataframe
    - Loads transformed Dataframe into MongoDB

    Whole process can be accessed via the `MongoETL.run` method

    Base class doesn't transform resulting data at all but provides
    hook for custom subclasses to perform their own custom transforms

    :param pipeline_func Function returning aggregation pipeline list. Will be invoked in `self.extract` step and passed
        **kwargs.
    :param input_mongo_collection String name of collection being queried.
    :param output_mongo_collection String name of collection into which results should be inserted.
    :param upsert: Whether to perform updates on existing documents matching key IDs or just perform a simple insert.
        Default is False.
    :param update_keys: List of fields in query results considered to be identifiers for update filter.
        If `upsert` == True, these must be provided.
    """
    def __init__(self, pipeline_func, input_mongo_collection, output_mongo_collection,
                 upsert=False, update_keys=None, query_options=None):
        self.pipeline_func = pipeline_func
        self.input_mongo_collection = input_mongo_collection
        self.output_mongo_collection = output_mongo_collection
        self.upsert = upsert
        self.update_keys = update_keys
        super(MongoAggregationETL, self).__init__(query_options=query_options)

    def run_query(self, pipeline, **kwargs):
        """
        Runs query against Mongo collection and returns result

        :param kwargs: passed to template
        :return:
        """
        return self.input_mongo_collection.aggregate(pipeline)

    def extract(self, **kwargs):
        """
        Run BigQuery query and load response into dataframe

        :param kwargs: all kwargs passed directly into template as template vars
        :return:
        """
        pipeline = self.pipeline_func(**kwargs)
        results = self.run_query(pipeline)
        results = list(results)
        logger.info('MongoDB aggregation pipeline against %s returned %s results'
                    % (self.input_mongo_collection, len(results)))
        return pd.DataFrame(list(results))

    def load(self, dataframe):
        """
        Loads a pandas dataframe object into MongoDB collection.

        If `self.upsert == True`, performs an individual row update using self.update_keys fields as
        filter for each row. If `self.upsert == False`, just performs an `insert_many`.

        :param dataframe:
        :return:
        """
        if dataframe.empty:
            return {}
        records = dataframe.to_dict(orient='records')
        if self.upsert:
            updated = 0
            inserted = 0
            upserts = []
            logger.info('Now upserting rows to collection %s...' % self.output_mongo_collection)
            for row in records:
                # first convert timestamp fields to datetime
                for k in row:
                    if isinstance(row[k], pd.tslib.Timestamp):
                        row[k] = row[k].to_datetime()
                # now perform update w/ upsert = true
                update_filter = {h: row[h] for h in self.update_keys}
                result = self.output_mongo_collection.update_one(update_filter, {"$set": row}, upsert=True)
                if result.matched_count:
                    updated += result.matched_count
                else:
                    inserted += 1
                upserts.append(result)
            logger.info('Upsert complete. Updated %s rows, inserted %s new ones.' % (updated, inserted))
            return upserts
        else:
            logger.info('Now inserting rows to collection %s...' % self.output_mongo_collection)
            # Not proud of this, would love to figure out a way
            # around this natively in Pandas
            for row in records:
                for k in row:
                    if isinstance(row[k], pd.tslib.Timestamp):
                        row[k] = row[k].to_datetime()
            res = self.output_mongo_collection.insert_many(records)
            logger.info('Insert complete, inserted %s rows.' % len(res.inserted_ids))
            return res


class DailyMongoAggregationETL(MongoAggregationETL):
    """
    For MongoDB to MongoDB ETLs performed for daily data aggregations. Only distinction from
    MongoAggregationETL is that `extract` step looks for a pre-specified date_field in the results
    and casts to datetime.

    TODO: Mongo >= 3.6 has date functions (like `$dateFromString` and `$dateFromParts`) that
    """
    def __init__(self, date_field, *args, **kwargs):
        self.date_field = date_field
        super(DailyMongoAggregationETL, self).__init__(*args, **kwargs)

    def extract(self, **kwargs):
        """
        Run BigQuery query and load response into dataframe

        :param kwargs: all kwargs passed directly into template as template vars
        :return:
        """
        pipeline = self.pipeline_func(**kwargs)
        results = self.run_query(pipeline)
        results = list(results)
        logger.info('MongoDB aggregation pipeline against %s returned %s results'
                    % (self.input_mongo_collection, len(results)))
        results = pd.DataFrame(results)
        # Cast date_field to date
        if not results.empty:
            results[self.date_field] = results[self.date_field].astype('datetime64[s]')
            logger.info('Date that will be inserted using as_type: %s' % results[self.date_field][0])
            # results[self.date_field] = pd.to_datetime(results[self.date_field][0], utc=True)
            # logger.info('Date that will be inserted using to_datetime: %s' % results[self.date_field][0])
        else:
            logger.info('No results returned from aggregation pipeline against %s, skipping remaining steps...'
                        % self.input_mongo_collection)
        return results
