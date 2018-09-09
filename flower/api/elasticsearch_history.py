from __future__ import absolute_import

from tornado import web

from elasticsearch import Elasticsearch, TransportError
from tornado.web import HTTPError

from ..options import options

from ..views import BaseHandler


class ElasticSearchHistoryHandler(BaseHandler):
    def __init__(self):
        ELASTICSEARCH_URL = options.elasticsearch_url
        self.es = Elasticsearch([ELASTICSEARCH_URL, ])
    @web.authenticated
    def post(self, index_name=None):
        index_name = index_name or 'task'
        try:
            self.es.indices.refresh(index_name)
        except TransportError as e:
            raise HTTPError(400, 'Invalid option: {}'.format(e))
        else:
            response = u'Successful refresh on index: {}'.format(index_name)
            self.write(response)
