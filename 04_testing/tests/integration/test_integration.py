import api
import scoring
import store
import os
import sys
import redis
import json
import time
import numpy as np
try:
    import mock
except ImportError:
    from unittest import mock

import unittest
import hashlib
import datetime
from tests.cases import cases

config = {
    'REDIS_LUNCH_SCRIPT': os.environ.get('REDIS_SERVER_LUNCHER')
}

redis_conn = redis.Redis(host='127.0.0.1', port=6379)

def setUpModule():
    os.system(config.get('REDIS_LUNCH_SCRIPT'))
    return True

def tearDownModule():
    os.system('pkill redis')
    if os.path.exists('./tests/integration/dump.rdb'):
        os.system('rm ./tests/integration/dump.rdb')


class TestIntegrate(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.settings = {'Store': store.Store()}
        # self.redis = redis.Redis(host='127.0.0.1', port=6379)


    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.settings)


    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            request_str = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(request_str.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()


    def set_online_score_data(self, case, connection):
        result = True
        for key, val in case.items():
            try:
                result &= connection.mset({key: val})
            except redis.exceptions.ConnectionError as e:
                print('Connection Error. Msg: {}'.format(e))
        return result


    def set_interests_request_data(self, case, connection):
        result = True
        interests_map = {
                    1: 'hi-tech',
                    2: 'books',
                    3: 'travel',
                    4: 'music',
                    5: 'sport',
                    6: 'art'
                    }
        if not case.get('client_ids'):
            return False
        for cid in case.get('client_ids'):
            a, b = np.random.choice([1, 2, 3, 4, 5, 6], 2, replace=False)
            result &= connection.mset({cid: json.dumps([interests_map.get(a), interests_map.get(b)])})
        return result


    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_integrate_ok_score_request(self, arguments):
        self.set_online_score_data(arguments, redis_conn)
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = float(response.get("score"))
        self.assertTrue(isinstance(score, (int, float)) and score >= 0, arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))


    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_integrate_ok_interests_request(self, arguments):
        self.set_interests_request_data(arguments, redis_conn)
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        self.assertEqual(len(arguments["client_ids"]), len(response))
        self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v) for v in response.values()))
        self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]))


    @cases([
        {"client_ids": [12, 14, 15], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [17, 16], "date": "19.07.2017"},
        {"client_ids": [15]},
    ])
    def test_integrate_not_found_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        self.assertEqual(len(arguments["client_ids"]), len(response))


class TestStoreOKIntegrate(unittest.TestCase):
    def setUp(self):
        self.store = store.Store()

    def set_key_to_redis(self, case):
        key, val, ex = case.get('key'), case.get('value'), case.get('ex')
        redis_conn.set(key, val, ex=ex)

    def get_uid_key(sel, key):
        phone, birthday, first_name, last_name = key.get('phone'), key.get('birthday'), \
                                                                key.get('first_name'), key.get('last_name')
        uid_key = scoring.get_key(first_name, last_name, phone, birthday)
        return uid_key


    @cases([
        {'key':{'phone': '79099934432', 'birthday':'01.01.1990', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 5.0, 'ex':60 * 60},
        {'key':{'phone': '79098438434', 'birthday':'01.01.1980', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.4, 'ex':60 * 60},
        {'key':{'phone': '79099845832', 'birthday':'', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.4, 'ex':60 * 60},
        {'key': {'phone': '79053457532', 'birthday': '10.12.1980', 'first_name': 'Vladimir',
                 'last_name': ''}, 'value': 3.4, 'ex': 60 * 60}
    ])
    def test_store_cache_set(self, case):
        case_key, val, ex = case.get('key'), case.get('value'), case.get('ex')
        key = self.get_uid_key(case_key)
        self.assertTrue(self.store.cache_set(key, val, ex))
        self.assertEqual(redis_conn.get(key), str(val).encode())
        if ex < 10:
            time.sleep(2) # sleep 2 seconds before check cached value
            self.assertEqual(redis_conn.get(key), None)


    @cases([
        {'key':{'phone': '79099934432', 'birthday':'01.01.1990', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 5.0},
        {'key':{'phone': '79098438434', 'birthday':'01.01.1980', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.4},
        {'key':{'phone': '79099845832', 'birthday':'', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.4},
        {'key': {'phone': '79053457532', 'birthday': '10.12.1980', 'first_name': 'Vladimir',
                 'last_name': ''}, 'value': 3.4}
    ])
    def test_store_cache_get(self, case):
        case_key, val = case.get('key'), case.get('value')
        key = self.get_uid_key(case_key)
        store_value = self.store.cache_get(key)
        self.assertEqual(store_value, str(val).encode())


    @cases([
        {'key':{'phone': '79099934432', 'birthday':'12.12.1985', 'first_name':'Oleg',
                'last_name':'Ivanov'}, 'value': 4.0},
        {'key':{'phone': '79098438434', 'birthday':'01.01.1980', 'first_name':'Petr',
                'last_name':'Sidorov'}, 'value': 4.0},
        {'key':{'phone': '79168884321', 'birthday':'01.01.1980', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.0},
        {'key': {'phone': '79053457532', 'birthday': '10.12.1980', 'first_name': '',
                 'last_name': ''}, 'value': 2.0}
    ])
    def test_store_set(self, case):
        case_key, val = case.get('key'), case.get('value')
        key = self.get_uid_key(case_key)
        self.assertTrue(self.store.set(key, val))
        self.assertEqual(redis_conn.get(key), str(val).encode())


    @cases([
        {'key':{'phone': '79099934432', 'birthday':'12.12.1985', 'first_name':'Oleg',
                'last_name':'Ivanov'}, 'value': 4.0},
        {'key':{'phone': '79098438434', 'birthday':'01.01.1980', 'first_name':'Petr',
                'last_name':'Sidorov'}, 'value': 4.0},
        {'key':{'phone': '79168884321', 'birthday':'01.01.1980', 'first_name':'Vladimir',
                'last_name':'Lavrenchenko'}, 'value': 3.0},
        {'key': {'phone': '79053457532', 'birthday': '10.12.1980', 'first_name': '',
                 'last_name': ''}, 'value': 2.0}
    ])
    def test_store_get(self, case):
        case_key, val = case.get('key'), case.get('value')
        key = self.get_uid_key(case_key)
        store_value = self.store.get(key)
        self.assertEqual(store_value, str(val).encode())


class TestStoreInvalid(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.timeouted_store = store.Store(host='127.0.1.1', port=6379)
        cls.conn_refused_store = store.Store(host='127.0.0.1', port=6380)


    @classmethod
    def tearDownClass(cls):
        cls.timeouted_store.disconnect()
        cls.conn_refused_store.disconnect()


    @cases([
        {'key': 'phone'},
        {'key': 'email'},
        {'key': 'first_name'}
    ])
    def test_invalid_get_timeout(self, case):
        key = case.get('key')
        with self.assertRaises(TimeoutError) as context:
            self.timeouted_store.get(key)

            self.assertRaises(TimeoutError, context.exception)


    @cases([
        {'key': 'phone'},
        {'key': 'email'},
        {'key': 'first_name'}
    ])
    def test_invalid_get_max_retry(self, case):
        key = case.get('key')
        with self.assertRaises(ConnectionError) as context:
            self.conn_refused_store.get(key)

            self.assertRaises(ConnectionError, context.exception)

        self.assertEqual(self.conn_refused_store.get_retry_count(), 5)


if __name__ == "__main__":
    unittest.main()
