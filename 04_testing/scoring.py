import hashlib
import json
import logging
import time
import random
import api
try:
    import redis
    print('redis successfully installed')
except ImportError:
    print('Need install redis module')
import fakeredis

class Store:
    def __init__(self, host='localhost', port=6379, db=0, defaut_timeout=5, reconnection_delay=0.01,
                                    reconnect_max_attempts=5, use_fake = False, fake_server=None, connection_now=True):
        self.host = host
        self.port = port
        self.attempt = 0
        self.default_timeout = defaut_timeout
        self.db = db
        self.reconnection_delay = reconnection_delay
        self.reconnect_max_attempts = reconnect_max_attempts
        self.connected = False
        self.use_fake = use_fake
        self.fake_server = fake_server
        self._connection = None
        if connection_now:
            self._connection = self.get_connection()


    def __repr__(self):
        # print('self.host', self.host)
        return 'Store: host:{}, port:{}, reconnection_delay: {}, reconnection_max_attempts: {}, connection: {}'.\
            format(self.host, self.port, self.reconnection_delay, self.reconnect_max_attempts, self._connection)


    def get_jitter_delay(self):
        return random.randint(1, 100) / 10000


    def get_connection(self, **kwargs):
        if not self.connected:
            self.attempt = 1
            # print('get_connection')
            while True:
                # print('self.attempt', self.attempt)
                try:
                    if self.use_fake:
                        # print('self.fake_server.connected', self.fake_server.connected)
                        r = fakeredis.FakeStrictRedis(server=self.fake_server, socket_timeout=self.default_timeout, \
                                                      socket_connect_timeout=self.default_timeout)
                    else:
                        r = redis.Redis(host=self.host, port=self.port, db=self.db, **kwargs)
                    # print('ping', r.ping())
                    self.connected = True
                    return r
                except redis.exceptions.ConnectionError as e:
                    if self.attempt >= self.reconnect_max_attempts:
                        return ''
                    msg = 'Cannot connect to clients Redis. err:{}'.format(e)
                    logging.error(msg)
                    self.attempt += 1
                    time.sleep(self.reconnection_delay + self.get_jitter_delay())
        else:
            return self._connection.pop()


    def cache_get(self, key):
        self.attempt = 1
        while True:
            if not self.connected:
                # time.sleep(self.reconnection_delay + self.get_jitter_delay())
                self._connection = self.get_connection()
                logging.info('Successfully reconnected')
            try:
                response = self._connection.get(key)
                break
            except redis.exceptions.ConnectionError as e:
                if self.attempt >= self.reconnect_max_attempts:
                    return ''
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e, host=self.host, port=self.port, attempt=self.attempt))
                self.attempt += 1
        if response:
            return response
        return False


    def cache_set(self, key, value, ex):
        self.attempt = 1
        while True:
            if not self.connected:
                # print(time.sleep(self.reconnection_delay + self.get_jitter_delay()))
                self._connection = self.get_connection()
                logging.info('Successfully reconnected')
            # print('ping', self._connection.ping())
            try:
                # self._connection.ping()
                response = self._connection.set(key, value, ex=ex)
                print('response', response)
                break
            except redis.exceptions.ConnectionError as e:
                if self.attempt >= self.reconnect_max_attempts:
                    return False
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                    error=e, host=self.host, port=self.port, attempt=self.attempt))
                self.attempt += 1
        if response:
            return response
        return False


    def get(self, key):
        self.attempt = 1
        while True:
            if not self.connected:
                time.sleep(self.reconnection_delay + self.get_jitter_delay())
                self._connection = self.get_connection()
                connected = True
                logging.info('Successfully reconnected')
            try:
                response = self._connection.get(key)
                break
            except redis.exceptions.ConnectionError as e:
                if self.attempt >= self.reconnect_max_attempts:
                    raise ConnectionError(e)
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e, host=self.host, port=self.port, attempt=self.attempt))
                self.attempt += 1
        if response:
            return response
        return False


    def set(self, key, value):
        # Нужно умень записывать какие-то значения в БД.
        # Что кромем key, value нужно установить в set()?
        self.attempt = 1
        while True:
            if not self.connected:
                time.sleep(self.reconnection_delay + self.get_jitter_delay())
                self._connection = self.get_connection()
                logging.info('Successfully reconnected')
            try:
                response = self._connection.set(key, value)
                break
            except redis.exceptions.ConnectionError as e:
                if self.attempt >= self.reconnect_max_attempts:
                    raise ConnectionError(e)
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e, host=self.host, port=self.port, attempt=self.attempt))
                self.attempt += 1
        if response:
            return response
        return False


    def get_retry_count(self):
        return self.attempt



def get_key(first_name, last_name, phone, birthday):
    key_parts = [
        first_name if first_name and not isinstance(first_name, api.Field) else "",
        last_name if last_name and not isinstance(last_name, api.Field) else "",
        str(phone) if phone and not isinstance(phone, api.Field) else "",
        birthday if birthday and not isinstance(birthday, api.Field) else "",
    ]
    print('scoring key_parts', key_parts)
    return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()

def get_score(store, phone=None, email=None, birthday=None, gender=None, first_name=None, last_name=None):
    print('Start get_score!')


    key = get_key(first_name, last_name, phone, birthday)

    # try get from cache,
    # fallback to heavy calculation in case of cache miss
    # print('scoring. call cache_get()')
    score = store.cache_get(key) or 0
    if score:
        return score
    if phone:
        score += 1.5
    if email:
        score += 1.5
    if birthday and gender:
        score += 1.5
    if first_name and last_name:
        score += 0.5
    # cache for 60 minutes
    # print('scoring. call cache_set()')
    store.cache_set(key, score, 60 * 60)
    return score


def get_interests(store, cid):
    try:
        r = store.get(cid)
    except Exception as e:
        logging.exception('Store unavaliable')
    return json.loads(r) if r else []


import os
import fakeredis
if __name__ == '__main__':
    # print(store)
    # os.system('./start-redis-server.sh')

    store = Store()

    print(store.cache_set('Alpha','1234', 60*60))
    # print('invalid get result: {}'.format(store.get('asdf')))
    # print('store set result', store.set('12345', json.dumps([1,2,2,3])))
    # print('store get', json.loads(store.get('12345')))
    # print('cache_get', store.cache_get('email'))
    # print(store.ping())
    # print(store.get_retry_count())
    os.system('pkill redis-server; rm dump.rdb')

    # server = fakeredis.FakeServer()
    # server.connected = False
    # r = fakeredis.FakeStrictRedis(server=server)


    # print(store.cache_get('Alpha'))

