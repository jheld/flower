from __future__ import absolute_import

import datetime
import logging

from functools import partial
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from tornado import web
from tornado import gen
from tornado import websocket
from tornado.ioloop import PeriodicCallback

from ..options import options
from ..views import BaseHandler
from ..api.workers import ListWorkers


logger = logging.getLogger(__name__)


class DashboardView(BaseHandler):
    es_dashboard_cache = dict()
    es_dashboard_update_time = datetime.datetime.utcnow()

    @web.authenticated
    @gen.coroutine
    def get(self):
        USE_ES = options.elasticsearch_dashboard
        refresh = self.get_argument('refresh', default=False, type=bool)
        json = self.get_argument('json', default=False, type=bool)

        app = self.application
        events = app.events.state
        broker = app.capp.connection().as_uri()

        if refresh:
            try:
                yield ListWorkers.update_workers(app=app)
            except Exception as e:
                logger.exception('Failed to update workers: %s', e)

        workers = {}
        from elasticsearch_dsl import Search
        from elasticsearch_dsl.query import Match
        from elasticsearch.client import Elasticsearch, TransportError
        use_es = USE_ES
        if use_es:
            es_client = Elasticsearch()
        else:
            es_client = None
        use_es = False
        for name, values in events.counter.items():
            if name not in events.workers:
                continue
            worker = events.workers[name]
            info = dict(values)
            if use_es:
                try:
                    # TODO: make this cache locking code actually work right.
                    if not self.es_dashboard_cache or self.es_dashboard_update_time <= datetime.datetime.utcnow() - datetime.timedelta(seconds=5):
                        pass
                    else:
                        info.update(**self.es_dashboard_cache)
                    es_s = Search(using=es_client, index='task')
                    started = es_s.query(Match(state='STARTED') & Match(hostname=name)).count()
                    processed = es_s.query(Match(state='RECEIVED') & Match(hostname=name)).count()
                    failed = es_s.query(Match(state='FAILED') & Match(hostname=name)).count()
                    retried = es_s.query(Match(state='RETRIED') & Match(hostname=name)).count()
                    succeeded = es_s.query(Match(state='SUCCESS') & Match(hostname=name)).count()
                    info['task-received'] = processed + started + succeeded + failed + retried
                    info['task-started'] = started
                    info['task-succeeded'] = succeeded
                    info['task-retried'] = retried
                    info['task-failed'] = failed
                    self.es_dashboard_cache['task-received'] = info['task-received']
                    self.es_dashboard_cache['started'] = info['task-started']
                    self.es_dashboard_cache['task-succeeded'] = info['task-succeeded']
                    self.es_dashboard_cache['task-retried'] = info['task-retried']
                    self.es_dashboard_cache['task-failed'] = info['task-failed']
                    self.es_dashboard_update_time = datetime.datetime.utcnow()
                except TransportError:
                    pass
            info.update(self._as_dict(worker))
            info.update(status=worker.alive)
            workers[name] = info

        if json:
            self.write(dict(data=list(workers.values())))
        else:
            self.render("dashboard.html", workers=workers, broker=broker)

    @classmethod
    def _as_dict(cls, worker):
        if hasattr(worker, '_fields'):
            return dict((k, worker.__getattribute__(k)) for k in worker._fields)
        else:
            return cls._info(worker)

    @classmethod
    def _info(cls, worker):
        _fields = ('hostname', 'pid', 'freq', 'heartbeats', 'clock',
                   'active', 'processed', 'loadavg', 'sw_ident',
                   'sw_ver', 'sw_sys')

        def _keys():
            for key in _fields:
                value = getattr(worker, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())


class DashboardUpdateHandler(websocket.WebSocketHandler):
    listeners = []
    periodic_callback = None
    workers = None
    page_update_interval = 2000

    def open(self):
        app = self.application
        if not app.options.auto_refresh:
            self.write_message({})
            return

        if not self.listeners:
            if self.periodic_callback is None:
                cls = DashboardUpdateHandler
                cls.periodic_callback = PeriodicCallback(
                    partial(cls.on_update_time, app),
                    self.page_update_interval)
            if not self.periodic_callback._running:
                logger.debug('Starting a timer for dashboard updates')
                self.periodic_callback.start()
        self.listeners.append(self)

    def on_message(self, message):
        pass

    def on_close(self):
        if self in self.listeners:
            self.listeners.remove(self)
        if not self.listeners and self.periodic_callback:
            logger.debug('Stopping dashboard updates timer')
            self.periodic_callback.stop()

    @classmethod
    def on_update_time(cls, app):
        update = cls.dashboard_update(app)
        if update:
            for l in cls.listeners:
                l.write_message(update)

    @classmethod
    def dashboard_update(cls, app):
        state = app.events.state
        workers = OrderedDict()
        use_es = USE_ES
        from elasticsearch_dsl import Search
        from elasticsearch_dsl.query import Match
        from elasticsearch.client import Elasticsearch, TransportError
        if use_es:
            es_client = Elasticsearch()
        else:
            es_client = None
        use_es = False
        for name, worker in sorted(state.workers.items()):
            if use_es:
                try:
                    es_s = Search(using=es_client, index='task')
                    started = es_s.query(Match(state='STARTED') & Match(hostname=name)).count()
                    processed = es_s.query(Match(state='RECEIVED') & Match(hostname=name)).count()
                    failed = es_s.query(Match(state='FAILED') & Match(hostname=name)).count()
                    retried = es_s.query(Match(state='RETRIED') & Match(hostname=name)).count()
                    succeeded = es_s.query(Match(state='SUCCESS') & Match(hostname=name)).count()
                    processed += succeeded + retried + failed + started
                except TransportError:
                    use_es = False
            if not use_es:
                counter = state.counter[name]
                started = counter.get('task-started', 0)
                processed = counter.get('task-received', 0)
                failed = counter.get('task-failed', 0)
                succeeded = counter.get('task-succeeded', 0)
                retried = counter.get('task-retried', 0)
            active = started - succeeded - failed - retried
            if active < 0:
                active = 'N/A'

            workers[name] = dict(
                name=name,
                status=worker.alive,
                active=active,
                processed=processed,
                failed=failed,
                succeeded=succeeded,
                retried=retried,
                loadavg=getattr(worker, 'loadavg', None))
        return workers

    def check_origin(self, origin):
        return True
