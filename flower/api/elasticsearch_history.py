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


class AlternativeBackendError(Exception):
    pass


def list_tasks_elastic_search(argument_getter):
    from elasticsearch import Elasticsearch, TransportError

    ELASTICSEARCH_URL = options.elasticsearch_url

    es = Elasticsearch([ELASTICSEARCH_URL, ])
    limit = argument_getter.get_argument('limit', None)
    worker = argument_getter.get_argument('workername', None)
    type = argument_getter.get_argument('taskname', None)
    state = argument_getter.get_argument('state', None)
    received_start = argument_getter.get_argument('received_start', None)
    received_end = argument_getter.get_argument('received_end', None)
    started_start = argument_getter.get_argument('started_start', None)
    started_end = argument_getter.get_argument('started_end', None)
    root_id = argument_getter.get_argument('root_id', None)
    parent_id = argument_getter.get_argument('parent_id', None)
    result = []
    limit = limit and int(limit)
    worker = worker if worker != 'All' else None
    type = type if type != 'All' else None
    state = state if state != 'All' else None
    from elasticsearch_dsl import Search
    from elasticsearch_dsl.query import Terms, Term, Match, Range
    s = Search(using=es, index='task')
    try:
        if worker:
            s = s.filter(Term(hostname=worker))
        if type:
            s = s.filter(Term(name=type))
        if state:
            s = s.filter(Term(state=state))
        if root_id:
            s = s.filter(Term(root_id=root_id))
        if parent_id:
            s = s.filter(Term(parent_id=parent_id))
        if received_start:
            s = s.filter(Range(received_time=dict(gt=received_start)))
        if received_end:
            s = s.filter(Range(received_time=dict(lt=received_end)))
            s = s.filter(Term(parent_id=parent_id))
        if started_start:
            s = s.filter(Range(started_time=dict(gt=started_start)))
        if started_end:
            s = s.filter(Range(started_time=dict(lt=started_end)))
        if limit is not None:
            s = s.extra(size=limit)
        hit_dicts = s.execute().hits.hits
        for hit_dict in hit_dicts:
            result.append((hit_dict['_id'], hit_dict['_source']))
    except TransportError as e:
        print(e)
        raise AlternativeBackendError()
    else:
        return result
