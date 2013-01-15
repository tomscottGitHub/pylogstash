import logging
import zmq
import socket
import datetime
import threading

MAX_MESSAGES = 1000


class Handler(logging.Handler):
    """ A logging handler for sending notifications to a 0mq PUSH.

    """
    def __init__(self, connect_string="tcp://127.0.0.1:2120", fields=[],
                 tags=[], input_type='pylogstash', context=None, queue_length=MAX_MESSAGES):
        logging.Handler.__init__(self)
        self._sockets = []
        self._context = context if context is not None else zmq.Context.instance()
        self._tags = tags
        self._connect_string = connect_string
        self._fields = fields
        self._input_type = input_type
        self._local = threading.local()
        self._queue_length = queue_length

    @property
    def publisher(self):
        if not hasattr(self._local, 'publisher'):
            print("creating publisher")
            # 0mq sockets aren't threadsafe, so bind them into a
            # threadlocal
            self._local.publisher = self._context.socket(zmq.PUB)
            self._local.publisher.setsockopt(zmq.HWM, self._queue_length)
            self._local.publisher.connect(self._connect_string)
        return self._local.publisher

    def emit(self, record):
        field_dict = dict([(field, getattr(record, field)) for field in self._fields if hasattr(record, field)])
        tags = []
        tags.extend(self._tags)
        tags.append('pylogstash')
        tags.extend(record.name.split("."))
        timestamp = datetime.datetime.utcfromtimestamp(record.created).isoformat()
        field_dict['timestamp'] = timestamp
        host = socket.gethostname()
        message = {
            "@source": record.filename,
            "@source_path": record.name,
            "@tags": tags,
            "@timestamp": timestamp,
            "@type": self._input_type,
            "@fields": field_dict,
            "@source_host": host,
            "@message": self.format(record)
        }
        self.publisher.send_json(message)
