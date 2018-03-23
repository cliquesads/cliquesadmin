from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
import datetime
import dateutil.parser
import pandas as pd
import redis
from cliquesadmin.jsonconfig import JsonConfigParser
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import os

config = JsonConfigParser()
mongo_host = config.get('Recommender', 'mongodb', 'host')
mongo_port = config.get('Recommender', 'mongodb', 'port')
mongo_user = config.get('Recommender', 'mongodb', 'user')
mongo_pwd = config.get('Recommender', 'mongodb', 'pwd')
db_name = config.get('Recommender', 'mongodb', 'db')
client = MongoClient(mongo_host, mongo_port)

db = client[db_name]
COLLECTION = db.articles

if os.environ.get('ENV', None) != 'local-test':
    db.authenticate(mongo_user, mongo_pwd, source=db_name)


def clean_text(text):
    # strip out non-ASCII chars
    text = ''.join([i if ord(i) < 128 else ' ' for i in text])
    # replace newline escape chars with spaces
    return text


def extract_article_text(soup, selector):
    main_content_block = soup.select(selector)
    if main_content_block:
        div = main_content_block[0]
        # Find all script tags and remove them
        scripts = div.find_all('script')
        for s in scripts:
            s.extract()
        return clean_text(div.get_text())
    else:
        raise LookupError("Content node not found.")


def _parse_meta_tags(soup, properties, namespace=None):

    def _coerce(key, val):
        if key.find("_time") > -1:
            try:
                val = dateutil.parser.parse(val)
            except ValueError:
                pass
        else:
            try:
                val = int(val)
            except ValueError:
                pass
        return val

    properties_dict = {}
    for p in properties:
        prop = "{}:{}".format(namespace, p) if namespace else p
        meta_tags = soup.find_all(property=prop)
        if meta_tags:
            if len(meta_tags) == 1:
                val = _coerce(prop, meta_tags[0].get('content'))
            else:
                val = [_coerce(prop, tag.get('content')) for tag in meta_tags]
            key = p.replace(':', "_")
            properties_dict[key] = val

    return properties_dict


def parse_opengraph_properties(soup):
    properties = [
        "title",
        "description",
        "locale",
        "type",
        "url",
        "site_name",
        "updated_time",
        "image",
        "image:secure_url",
        "image:width"
        "image:height"
    ]
    namespace = "og"
    return _parse_meta_tags(soup, properties, namespace=namespace)


def parse_article_properties(soup):
    properties = [
        "tag",
        "publisher",
        "section",
        "published_time",
        "modified_time",
    ]
    namespace = "article"
    return _parse_meta_tags(soup, properties, namespace=namespace)


def get_date_from_selector(soup, selector):
    dt = soup.select(selector)[0].get_text()
    try:
        dt = dateutil.parser.parse(dt)
    except ValueError:
        print "Tried to parse date from selector but invalid datetime string was received, skipping."
    return dt


def walk_article_dir_and_store_text(root, basepath, collection, selectors, publisher, site):

    # first get selectors to be used when looking for article attributes
    text_selector = selectors["text"] #text selector is mandatory
    # published_time selector used to find datetime article was published if this is
    # not available in meta
    published_time_selector = selectors["published_time"] if selectors.has_key("published_time") else False

    for root, dirs, files in os.walk(root):
        for name in files:
            filepath = os.path.join(root, name)
            f = open(filepath, 'r')
            contents = f.read()
            soup = BeautifulSoup(contents, 'html.parser')
            # Try to extract text, otherwise log it and move on
            try:
                text = extract_article_text(soup, text_selector)
                url = root.replace(basepath, 'http:/')
                opengraph = parse_opengraph_properties(soup)
                article = parse_article_properties(soup)

                # check published_time selector if published_time not available in meta
                if not(opengraph.has_key('published_time')) and not(article.has_key('published_time')):
                    if published_time_selector:
                        dt = get_date_from_selector(soup, published_time_selector)
                        opengraph["published_time"] = dt
                        article["published_time"] = dt

                obj = {
                    "tstamp": datetime.datetime.utcnow(),
                    "text": text,
                    "url": url,
                    "publisher": publisher,
                    "site": site,
                    "meta": {
                        "opengraph": opengraph,
                        "article": article
                    }
                }
                collection.insert_one(obj)
                print "Successfully extracted & stored text from {}".format(url)

            except LookupError:
                print "Whoops, could not find .entry_content div in {}".format(filepath)


class ContentEngine(object):

    SIMKEY = 'p:smlr:%s'

    def __init__(self):
        self._r = redis.StrictRedis(host='localhost', port=6379, db=0)
        self._client = client

    def train(self, cursor):
        start = time.time()
        ds = pd.DataFrame(list(cursor))
        print("Training data ingested in %s seconds." % (time.time() - start))

        # Flush the stale training data from redis
        self._r.flushdb()

        start = time.time()
        self._train(ds)
        print("Engine trained in %s seconds." % (time.time() - start))

    def _train(self, ds):
        """
        Train the engine.

        Create a TF-IDF matrix of unigrams, bigrams, and trigrams
        for each product. The 'stop_words' param tells the TF-IDF
        module to ignore common english words like 'the', etc.

        Then we compute similarity between all products using
        SciKit Leanr's linear_kernel (which in this case is
        equivalent to cosine similarity).

        Iterate through each item's similar items and store the
        100 most-similar. Stops at 100 because well...  how many
        similar products do you really need to show?

        Similarities and their scores are stored in redis as a
        Sorted Set, with one set for each item.

        :param ds: A pandas dataset containing two fields: description & id
        :return: Nothin!
        """

        tf = TfidfVectorizer(analyzer='word',
                             ngram_range=(1, 3),
                             min_df=0,
                             stop_words='english')
        tfidf_matrix = tf.fit_transform(ds['text'])

        cosine_similarities = linear_kernel(tfidf_matrix, tfidf_matrix)

        for idx, row in ds.iterrows():
            similar_indices = cosine_similarities[idx].argsort()[:-10:-1]
            similar_items = [(cosine_similarities[idx][i], ds['_id'][i])
                             for i in similar_indices]

            # First item is the item itself, so remove it.
            # This 'sum' is turns a list of tuples into a single tuple:
            # [(1,2), (3,4)] -> (1,2,3,4)
            flattened = sum(similar_items[1:], ())
            self._r.zadd(self.SIMKEY % row['_id'], *flattened)

    def predict(self, item_id, num):
        """
        Couldn't be simpler! Just retrieves the similar items and
        their 'score' from redis.

        :param item_id: string
        :param num: number of similar items to return
        :return: A list of lists like: [["19", 0.2203],
        ["494", 0.1693], ...]. The first item in each sub-list is
        the item ID and the second is the similarity score. Sorted
        by similarity score, descending.
        """

        return self._r.zrange(self.SIMKEY % item_id,
                              0,
                              num-1,
                              withscores=True,
                              desc=True)

content_engine = ContentEngine()

if __name__ == "__main__":
    HOME = '/Users/bliang'
    UNQUALIFIED_URL = 'www.smartertravel.com'
    SELECTORS = {
        "text": '.entry-content',
        # "published_time": '.blog_entry_date'
    }
    BASEPATH = os.path.join(HOME, UNQUALIFIED_URL)

    ARTICLE_SUBDIRECTORIES = [str(yr) for yr in range(2007, 2019)]
    PUBLISHER = '59b9bdae3cd9be16b68da219'
    SITE = '59b9bdae3cd9be16b68da21a'

    # Parse raw HTML files
    for subdir in ARTICLE_SUBDIRECTORIES:
        walk_article_dir_and_store_text(os.path.join(BASEPATH, subdir),
                                        HOME,
                                        COLLECTION,
                                        SELECTORS,
                                        PUBLISHER,
                                        SITE)

    # Train the model
    content_engine = ContentEngine()
    content_engine.train(COLLECTION.find())

    # Store model weights to MongoDB
    for doc in COLLECTION.find():
        tf_idf = content_engine.predict(str(doc["_id"]), 0)
        doc["tf_idf"] = [{"article": t[0], "weight": t[1]} for t in tf_idf]
        COLLECTION.save(doc)
