# -*- coding: utf-8 -*-
import datetime
import json
import logging
import elasticsearch
import redis

class CastorConfig(object):

    def __init__(self):
        try:
            with open('config.json') as f:
                config = f.read()
                self.raw_config = json.loads(config)
        except IOError as e:
            raise e
        except ValueError as e:
            raise e

    def get(self, key):
        if key in self.raw_config:
            return self.raw_config[key]
        else:
            return self.default_config.get(key, None)

    default_config = {
        'redis_hostname': '127.0.0.1',
        'redis_port': '6379',
        'redis_db': 1,
        'redis_namespaces': ['castor:logs'],
        'es_hostname': '127.0.0.1',
        'es_port': '9200'
    }

class Castor(object):

    def __init__(self):
        self.config = CastorConfig()
        self._redis = redis.StrictRedis(
            host=self.config.get('redis_hostname'),
            port=int(self.config.get('redis_port')),
            db=int(self.config.get('redis_db')),
            socket_timeout=10
        )
        self._es = elasticsearch.Elasticsearch(hosts=[{
            'host': self.config.get('es_hostname'),
            'port': int(self.config.get('es_port'))
        }])
        self.try_redis()
        self.try_elasticsearch()
        self.start_watching()

    def try_redis(self):
        try:
            self._redis.ping()
        except UserWarning as e:
            raise e
        except Exception as e:
            raise e

    def try_elasticsearch(self):
        try:
            self._es.cluster.health()
        except elasticsearch.exceptions.ConnectionError as e:
            raise e

    def start_watching(self):
        while 1:
            try:
                key, msg = self._redis.blpop(self.config.get('redis_namespaces'))
            except redis.exceptions.ConnectionError:
                continue
            try:
                msg = json.loads(msg)
            except ValueError as e:
                logger.exception(e)
                continue
            today_index = 'logstash-%s' % datetime.datetime.now().strftime('%Y.%m.%d')
            self._es.index(index=today_index, doc_type='castor', body=msg)


def set_logging():
    log_format = logging.Formatter('%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s')
    logger = logging.getLogger('castor')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('castor.log')
    fh.setFormatter(log_format)
    ch = logging.StreamHandler()
    ch.setFormatter(log_format)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


if __name__ == '__main__':
    logger = set_logging()
    try:
        logger.info('Launching Castor...')
        castor = Castor()
    except KeyboardInterrupt:
        pass
