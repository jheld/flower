from __future__ import absolute_import

import json
import logging
import threading
import time
import traceback
from datetime import datetime
from logging import config
from .options import options

import elasticsearch
import pytz
from elasticsearch import RequestsHttpConnection, TransportError
from elasticsearch.client import IndicesClient, Elasticsearch
from elasticsearch.helpers import bulk

try:
    import queue
except:
    import Queue as queue


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'class': 'flower.logging_utils.CeleryOneLineExceptionFormatter',
            'format': '%(levelname)s %(asctime)s %(funcName)s %(module)s %(lineno)d %(message)s'
        },
    },
    'handlers': {
        'task_logger_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'task_logger.log',
            'formatter': 'verbose',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 30,
            'utc': True,
        },
        'stream': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'task_logger': {
            'handlers': ['task_logger_file', 'stream', ],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}
config.dictConfig(LOGGING)

logger = logging.getLogger('task_logger')
ELASTICSEARCH_URL = options.elasticsearch_url
ES_INDEX_TIMEOUT = options.elasticsearch_index_timeout
ES_INDEX_BULK_SIZE = options.elasticsearch_index_bulk_size


ES_CLIENT = Elasticsearch(
    [ELASTICSEARCH_URL],
    connection_class=RequestsHttpConnection
)

indices_client = IndicesClient(client=ES_CLIENT)
# Index name states the day in which it was created -- not how long it is around for
index_name = 'task-{}'.format(datetime.utcnow().date().isoformat())
# create or recreate index
# if indices_client.exists(index_name):
#     indices_client.delete(index=index_name)
try:
    indices_client.create(index=index_name)
except TransportError as te:
    if te.error == 'index_already_exists_exception':
        pass
    else:
        print(traceback.print_exc())
try:
    if indices_client.exists(index=index_name):
        indices_client.put_alias('task-*', 'task')
except TransportError as te:
    print(traceback.print_exc())
body = {
    'properties': {
        'hostname': {'type': 'keyword', },
        'worker': {'type': 'keyword', },
        'clock': {'type': 'integer', },
        'args': {'type': 'keyword', },
        'kwargs': {'type': 'keyword', },
        'timestamp_time': {'type': 'date', },
        'timestamp': {'type': 'float', },
        'root_id': {'type': 'keyword', },
        'root': {'type': 'keyword', },
        'parent_id': {'type': 'keyword', },
        'name': {'type': 'keyword', },
        'result': {'type': 'keyword', },
        'state': {'type': 'keyword', },
        'eta': {'type': 'date', },
        'received': {'type': 'float', },
        'retries': {'type': 'integer', },
        'received_time': {
            "type": "date",
        },
        'expires': {'type': 'date', },
        'revoked': {'type': 'float', },
        'revoked_time': {
            "type": "date",
        },
        'retried': {'type': 'float', },
        'retried_time': {
            "type": "date",
        },
        'started': {'type': 'float', },
        'started_time': {
            "type": "date",
        },
        'failed': {'type': 'float', },
        'failed_time': {
            "type": "date",
        },
        'succeeded': {'type': 'float', },
        'succeeded_time': {
            "type": "date",
        },
        'runtime': {'type': 'float', },
        'info': {'type': 'text', },
        'traceback': {'type': 'text', },
        'exception': {'type': 'text', },
        '_fields': {'type': 'keyword', },
        'children': {'type': 'keyword', },
    }
}

es_queue = queue.Queue()


def es_consumer():
    es_buffer = []
    while True:
        start_time = int(time.time())
        try:
            while len(es_buffer) < ES_INDEX_BULK_SIZE:
                es_buffer.append(es_queue.get(timeout=ES_INDEX_TIMEOUT))
                es_queue.task_done()
                got_task_time = int(time.time())
                if got_task_time - start_time >= ES_INDEX_TIMEOUT:
                    raise queue.Empty
        except queue.Empty:
            pass
        if es_buffer:
            for try_idx in range(3):
                # should consider implementing retry logic (outside of what the ES library uses)
                try:
                    bulk(actions=es_buffer, client=ES_CLIENT, stats_only=True)
                except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout, ):
                    time.sleep(pow(2, try_idx))
                except elasticsearch.helpers.BulkIndexError:
                    print(traceback.format_exc())
                    break
                except Exception:
                    es_buffer[:] = []
                    print(traceback.format_exc())
                    break
                else:
                    es_buffer[:] = []
                    break
            # Can enable the sleep in case it seems like we're writing into ES too frequently
            # time.sleep(0.5)


if options.elasticsearch_index:
    es_thread = threading.Thread(target=es_consumer)
    es_thread.daemon = True
    es_thread.start()


def send_to_elastic_search(state, event):
    # task name is sent only with -received event, and state
    # will keep track of this for us.
    if not event['type'].startswith('task-'):
        return
    task = state.tasks.get(event['uuid'])
    received_time = task.received
    succeeded_time = task.succeeded
    start_time = task.started

    # potentially use the sched module to change it via native python logic
    active_index_name = 'task-{}'.format(datetime.now(tz=pytz.utc).date().isoformat())
    global index_name
    if active_index_name != index_name:
        try:
            indices_client.create(index=active_index_name)
            indices_client.put_alias('task-*', 'task')
            indices_client.put_mapping(
                doc_type='task',
                body=body,
                index=active_index_name
            )
            index_name = active_index_name
        except TransportError as te:
            print(traceback.print_exc())

    doc_body = {
        'hostname': task.hostname,
        'worker': task.hostname if task.worker else None,
        'exchange': task.exchange,
        'retries': task.retries,
        'routing_key': task.routing_key,
        'args': task.args,
        'kwargs': task.kwargs,
        'name': task.name,
        'clock': task.clock,
        'children': str(task.children) if task.children else None,
        'expires': task.expires if task.expires else task.expires,
        'eta': task.eta,
        'state': task.state,
        'received': received_time,
        'received_time': datetime.utcfromtimestamp(received_time).replace(tzinfo=pytz.utc) if received_time else None,
        'retried': task.retried,
        'retried_time': datetime.utcfromtimestamp(task.retried).replace(tzinfo=pytz.utc) if task.retried else None,
        'started': start_time,
        'started_time': datetime.utcfromtimestamp(start_time).replace(tzinfo=pytz.utc) if start_time else None,
        'succeeded': succeeded_time,
        'succeeded_time': datetime.utcfromtimestamp(succeeded_time).replace(
            tzinfo=pytz.utc) if succeeded_time else None,
        'revoked': task.revoked,
        'revoked_time': datetime.utcfromtimestamp(task.revoked).replace(tzinfo=pytz.utc) if task.revoked else None,
        'failed': task.failed,
        'failed_time': datetime.utcfromtimestamp(task.failed).replace(tzinfo=pytz.utc) if task.failed else None,
        'info': json.dumps(task.info()),
        'result': task.result,
        'root_id': task.root_id,
        'root': str(task.root) if task.root else None,
        'runtime': task.runtime,
        'timestamp': task.timestamp,
        'timestamp_time': datetime.utcfromtimestamp(task.timestamp).replace(tzinfo=pytz.utc) if task.timestamp else None,
        'exception': task.exception,
        'traceback': task.traceback,
        'parent_id': task.parent_id,
        '_fields': task._fields,
    }
    try:
        doc_body['_type'] = 'task'
        doc_body['_op_type'] = 'index'
        doc_body['_index'] = index_name
        doc_body['_id'] = task.uuid
        es_queue.put(doc_body)
        # ES_CLIENT.index(index_name, index_name, id=task.uuid, body=doc_body)
    except Exception:
        logger.info('{name}[{uuid}] worker: {worker}, received: {received}, '
                    'started: {started}, succeeded: {succeeded}, info={info}'.format(name=task.name,
                                                                                     uuid=task.uuid,
                                                                                     worker=task.hostname,
                                                                                     info=task.info(),
                                                                                     received=received_time,
                                                                                     started=start_time,
                                                                                     succeeded=succeeded_time, ))


def my_monitor(celery_app):
    # good during development, but we may not always want to do put_mapping.
    indices_client.put_mapping(
        doc_type='task',
        body=body,
        index=index_name
    )
    state = celery_app.events.State(max_tasks_in_memory=options.max_tasks)

    def event_handler(event):
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        # task = state.tasks.get(event['uuid'])
        state.event(event)
        send_to_elastic_search(state, event)

    with celery_app.connection() as connection:
        recv = celery_app.events.Receiver(connection, handlers={
            'task-failed': event_handler,
            'task-succeeded': event_handler,
            'task-received': event_handler,
            'task-retried': event_handler,
            'task-started': event_handler,
            'task-revoked': event_handler,
            '*': state.event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)


# if __name__ == '__main__':
#     flower = FlowerCommand()
#     argv = list(sys.argv)
#     flower.setup_app_from_commandline(argv=argv)
#     app = flower.app
#     my_monitor(app)
