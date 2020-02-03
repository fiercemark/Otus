import hashlib
import json
import logging
import time
import random
try:
    import redis
    print('redis successfully installed')
except ImportError:
    print('Need install redis module')

class Store:
    def __init__(self, host='localhost', port=6379, db=0, defaut_timeout=5, reconnection_delay=0.01, reconnect_max_attempts=5):
        self.host = host
        self.port = port
        self.default_timeout = defaut_timeout
        self.db = db
        self.reconnection_delay = reconnection_delay
        self.reconnect_max_attempts = reconnect_max_attempts
        # self.password = '1234'
        self.connected = False
        self._connection = self.get_connection()


    def __repr__(self):
        print('self.host', self.host)
        return 'Store: host:{}, port:{}, reconnection_delay: {}, reconnection_max_attempts: {}, connection: {}'.\
            format(self.host, self.port, self.reconnection_delay, self.reconnect_max_attempts, self._connection)

    @staticmethod
    def get_jitter_delay():
        return random.randint(1, 100) / 10000


    def get_connection(self, **kwargs):
        if not self.connected:
            while True:
                try:
                    return redis.Redis(host=self.host, port=self.port,
                                       db=self.db, **kwargs)
                except Exception as e:
                    msg = 'Cannot connect to clients Redis. err:{}'.format(e)
                    logging.error(msg)
                    time.sleep(self.reconnection_delay + self.get_jitter_delay)
        else:
            return self._connection.pop()


    def cache_get(self, key):
        connected = True
        attempt = 1
        while True:
            try:
                if not connected:
                    time.sleep(self.reconnection_delay + self.get_jitter_dela)
                    self._connection = self.get_connection()
                    connected = True
                    logging.info('Successfully reconnected')
                response = self._connection.get(key)
                break
            except Exception as e:
                if attempt > self.reconnect_max_attempts:
                    return ''
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e.message, host=self.host, port=self.port, attempt=attempt))
                attempt += 1
                connected = False
        if connected:
            return response
        return ''


    def cache_set(self, key, value, ex):
        connected = True
        attempt = 1
        while True:
            try:
                if not connected:
                    time.sleep(self.reconnection_delay + self.get_jitter_dela)
                    self._connection = self.get_connection()
                    connected = True
                    logging.info('Successfully reconnected')
                response = self._connection.set(key, value, ex=ex)
                break
            except Exception as e:
                if attempt > self.reconnect_max_attempts:
                    return False
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                    error=e.message, host=self.host, port=self.port, attempt=attempt))
                attempt += 1
                connected = False
        if connected:
            return response
        return False


    def get(self, key):
        connected = True
        attempt = 1
        while True:
            try:
                if not connected:
                    time.sleep(self.reconnection_delay + self.get_jitter_dela)
                    self._connection = self.get_connection()
                    connected = True
                    logging.info('Successfully reconnected')
                response = self._connection.get(key)
                if not response:
                    response = ''
                break
            except Exception as e:
                if attempt > self.reconnect_max_attempts:
                    raise
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e.message, host=self.host, port=self.port, attempt=attempt))
                attempt += 1
                connected = False
        return response


    def set(self, key, value):
        # Нужно умень записывать какие-то значения в БД.
        # Что кромем key, value нужно установить в set()?
        connected = True
        attempt = 1
        while True:
            try:
                if not connected:
                    time.sleep(self.reconnection_delay + self.get_jitter_dela)
                    self._connection = self.get_connection()
                    connected = True
                    logging.info('Successfully reconnected')
                response = self._connection.set(key, value)
                if not response:
                    response = ''
                break
            except Exception as e:
                if attempt > self.reconnect_max_attempts:
                    raise
                logging.info('Error:{error}. Reconnected to {host}:{port}. Attempt: {attempt}'.format(
                                            error=e.message, host=self.host, port=self.port, attempt=attempt))
                attempt += 1
                connected = False


def get_key(key_parts):
    return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()

def get_score(store, phone, email, birthday=None, gender=None, first_name=None, last_name=None):
    print('Start get_score!')
    key_parts = [
        first_name or "",
        last_name or "",
        str(phone) if phone else "",
        birthday or "",
    ]
    print('scoring key_parts', key_parts)
    key = get_key(key_parts)

    # try get from cache,
    # fallback to heavy calculation in case of cache miss
    print('scoring. call cache_get()')
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
    print('scoring. call cache_set()')
    store.cache_set(key, score, 60 * 60)
    return score


def get_interests(store, cid):
    try:
        r = store.get("i:%s" % cid)
    except Exception as e:
        logging.exception('Store unavaliable')
    return json.loads(r) if r else []


if __name__ == '__main__':
    store = Store()
    # print(store)
    store.cache_set('Alpha','1234', 60*60)
    print(store.cache_get('Alpha'))
