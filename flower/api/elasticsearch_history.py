import sys

from tornado import web

from elasticsearch import Elasticsearch, TransportError
from tornado.web import HTTPError

from ..views import BaseHandler

try:
    ELASTICSEARCH_URL = str(next(item.split('=')[-1] for item in sys.argv if '--elasticsearch-url' in item))
except StopIteration:
    ELASTICSEARCH_URL = 'http://localhost:9200/'
else:
    ELASTICSEARCH_URL = ELASTICSEARCH_URL or 'http://localhost:9200/'

es = Elasticsearch([ELASTICSEARCH_URL, ])


class ElasticSearchHistoryHandler(BaseHandler):
    @web.authenticated
    def post(self, index_name=None):
        index_name = index_name or 'task'
        try:
            es.indices.refresh(index_name)
        except TransportError as e:
            raise HTTPError(400, 'Invalid option: {}'.format(e))
        else:
            response = u'Successful refresh on index: {}'.format(index_name)
            self.write(response)
