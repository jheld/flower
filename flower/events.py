from __future__ import absolute_import
from __future__ import with_statement

import time
import shelve
import logging
import threading
import collections

from functools import partial

import celery

from pkg_resources import parse_version

from tornado.ioloop import PeriodicCallback
from tornado.ioloop import IOLoop

from celery.events import EventReceiver
from celery.events.state import State

from . import api

from .options import options

try:
    from collections import Counter
except ImportError:
    from .utils.backports.collections import Counter


logger = logging.getLogger(__name__)


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)

    def event(self, event):
        worker_name = event['hostname']
        event_type = event['type']

        if not self.counter[worker_name] and options.elasticsearch_dashboard:
            from elasticsearch import Elasticsearch
            ELASTICSEARCH_URL = options.elasticsearch_url
            es = Elasticsearch([ELASTICSEARCH_URL, ])
            from elasticsearch_dsl import Search
            from elasticsearch_dsl.query import Term
            s = Search(using=es, index='task')
            started = s.filter(Term(state='STARTED') & Term(hostname=worker_name)).count()
            processed = s.filter(Term(state='RECEIVED') & Term(hostname=worker_name)).count()
            failed = s.filter(Term(state='FAILED') & Term(hostname=worker_name)).count()
            retried = s.filter(Term(state='RETRIED') & Term(hostname=worker_name)).count()
            succeeded = s.filter(Term(state='SUCCESS') & Term(hostname=worker_name)).count()
            self.counter[worker_name]['task-received'] = processed + started + succeeded + failed + retried
            self.counter[worker_name]['task-started'] = started
            self.counter[worker_name]['task-succeeded'] = succeeded
            self.counter[worker_name]['task-retried'] = retried
            self.counter[worker_name]['task-failed'] = failed
            if not event_type.startswith('task-'):
                self.counter[worker_name][event_type] += 1
        else:
            self.counter[worker_name][event_type] += 1

        # Send event to api subscribers (via websockets)
        classname = api.events.getClassName(event_type)
        cls = getattr(api.events, classname, None)
        if cls:
            cls.send_message(event)

        # Save the event
        super(EventsState, self).event(event)

        # from .elasticsearch_history import send_to_elastic_search
        # try:
        #     send_to_elastic_search(self, event)
        # except Exception as e:
        #     print(e)


class Events(threading.Thread):
    events_enable_interval = 5000

    def __init__(self, capp, db=None, persistent=False,
                 enable_events=True, io_loop=None, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop or IOLoop.instance()
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.enable_events = enable_events
        self.state = None

        if self.persistent and parse_version(celery.__version__) < parse_version("3.0.15"):
            logger.warning('Persistent mode is available with '
                           'Celery 3.0.15 and later')
            self.persistent = False

        if self.persistent:
            logger.debug("Loading state from '%s'...", self.db)
            state = shelve.open(self.db)
            if state:
                self.state = state['events']
            state.close()

        if not self.state:
            self.state = EventsState(**kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        # Celery versions prior to 2 don't support enable_events
        if self.enable_events and celery.VERSION[0] > 2:
            self.timer.start()

    def stop(self):
        if self.persistent:
            logger.debug("Saving state to '%s'...", self.db)
            state = shelve.open(self.db)
            state['events'] = self.state
            state.close()

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    try_interval = 1
                    recv.capture(limit=None, timeout=None, wakeup=True)

            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            self.capp.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
