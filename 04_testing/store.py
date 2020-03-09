import os
import time
import random
import logging
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

    def disconnect(self):
        self._connection.close()


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
                        r = redis.Redis(host=self.host, port=self.port, db=self.db,
                                        socket_connect_timeout= self.default_timeout, **kwargs)
                    self.connected = True
                    return r
                except redis.exceptions.ConnectionError as e:
                    if self.attempt >= self.reconnect_max_attempts:
                        return ''
                    msg = 'Cannot connect to clients Redis. err:{}'.format(e)
                    logging.error(msg)
                    self.attempt += 1
                    time.sleep(self.reconnection_delay + self.get_jitter_delay())
                except redis.exceptions.TimeoutError as e:
                    logging.info('Error:{error}. TimeoutError to {host}:{port}'.format(
                        error=e, host=self.host, port=self.port))
                    raise TimeoutError(e)
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
            except redis.exceptions.TimeoutError as e:
                logging.info('Error:{error}. TimeoutError to {host}:{port}'.format(
                                            error=e, host=self.host, port=self.port))
                raise TimeoutError(e)
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
            except redis.exceptions.TimeoutError as e:
                logging.info('Error:{error}. TimeoutError to {host}:{port}'.format(
                                            error=e, host=self.host, port=self.port))
                raise TimeoutError(e)
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
            except redis.exceptions.TimeoutError as e:
                logging.info('Error:{error}. TimeoutError to {host}:{port}'.format(
                                            error=e, host=self.host, port=self.port))
                raise TimeoutError(e)

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
                    raise ConnectionError('Fatal error store get. Msg:{}'.format(e))
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e, host=self.host, port=self.port, attempt=self.attempt))
                self.attempt += 1
            except redis.exceptions.TimeoutError as e:
                logging.info('Error:{error}. TimeoutError to {host}:{port}'.format(
                                            error=e, host=self.host, port=self.port))
                raise TimeoutError(e)
        if response:
            return response
        return False


    def get_retry_count(self):
        return self.attempt
