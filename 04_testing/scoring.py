import hashlib
import json
import logging
import time
import random
import api
try:
    import redis
    logging.info('redis successfully installed')
except ImportError:
    logging.info('Need install redis module')
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
        return 'Store: host:{}, port:{}, reconnection_delay: {}, reconnection_max_attempts: {}, connection: {}'.\
            format(self.host, self.port, self.reconnection_delay, self.reconnect_max_attempts, self._connection)


    def get_jitter_delay(self):
        return random.randint(1, 100) / 10000


    def get_connection(self, **kwargs):
        if not self.connected:
            self.attempt = 1
            while True:
                try:
                    if self.use_fake:
                        r = fakeredis.FakeStrictRedis(server=self.fake_server, socket_timeout=self.default_timeout, \
                                                      socket_connect_timeout=self.default_timeout)
                    else:
                        r = redis.Redis(host=self.host, port=self.port, db=self.db, **kwargs)
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
                self._connection = self.get_connection()
                logging.info('Successfully reconnected')
            try:
                # self._connection.ping()
                response = self._connection.set(key, value, ex=ex)
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
    return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()


def get_score(store, phone=None, email=None, birthday=None, gender=None, first_name=None, last_name=None):
    logging.info('Start get_score!')

    key = get_key(first_name, last_name, phone, birthday)
    # try get from cache,
    # fallback to heavy calculation in case of cache miss
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
    store.cache_set(key, score, 60 * 60)
    return score


def get_interests(store, cid):
    try:
        r = store.get(cid)
    except Exception as e:
        logging.exception('Store unavaliable')
    return json.loads(r) if r else []