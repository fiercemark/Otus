import api
import scoring
import store
import os
import json
import numpy as np
try:
    import mock
except ImportError:
    from unittest import mock

import unittest
import hashlib
import datetime
import fakeredis
from tests.cases import cases


class MockStore:
    def __init__(self):
        self.data = {}


    def cache_get(self, key):
        if key not in self.data:
            return ''
        else:
            return self.data[key]


    def cache_set(self, key, value, time):
        self.data[key] = value

def set_iterests(argumets, store):
    result = True
    interests_map = {
                1: 'hi-tech',
                2: 'books',
                3: 'travel',
                4: 'music',
                5: 'sport',
                6: 'art'
                }
    if not argumets.get('client_ids'):
        return False
    clients_ids = argumets.get('client_ids')
    for cid in clients_ids:
        a, b = np.random.choice([1, 2, 3, 4, 5, 6], 2, replace=False)
        result &= store.mset({cid: json.dumps([interests_map.get(a), interests_map.get(b)])})
    return result

class TestClassField(unittest.TestCase):
    def setUp(self):
        self.f = api.Field(required=True)


    @cases(['', []])
    def test_field_attribute_fail(self, case):
        with self.assertRaises(ValueError) as context:
            self.f.parse_validate(case)

            self.assertRaises(ValueError, context.exception)


    @cases(['value','value_another'])
    def test_field_attribute_ok(self, case):
        self.assertEqual(self.f.parse_validate(case), case)


class TestClassCharField(unittest.TestCase):
    def setUp(self):
        self.cf = api.CharField(required=False, nullable=True)


    @cases(['vladimir', 'vladimir lavrenchenko', 'sdhfjy45y3u54545345', 'token_bbl', 'some_method'])
    def test_charfield_attribute_ok(self, case):
        self.assertEqual(self.cf.parse_validate(case), case)


    @cases([1223, []])
    def test_charfield_attribute_fail(self, case):
        with self.assertRaises(ValueError) as context:
            self.cf.parse_validate(case)

            self.assertRaises(ValueError, context.exception)

class TestClassArgumentsField(unittest.TestCase):
    def setUp(self):
        self.af = api.ArgumentsField(required=True, nullable=True)


    @cases([{}, {"phone": "79175002040", "email": "stupnikov@otus.ru"}])
    def test_arguments_field_ok(self, case):
        self.assertEqual(self.af.parse_validate(case), case)


    @cases(['', [], 1234, 'asdfasdf'])
    def test_arguments_field_fail(self, case):
        with self.assertRaises(ValueError) as context:
            self.af.parse_validate(case)

            self.assertRaises(ValueError, context.exception)

class TestEmailField(unittest.TestCase):
    def setUp(self):
        self.ef = api.EmailField(required=False, nullable=True)


    @cases(['sdfsd@sdfd.sdfdf', 'vlavenko@gmail.com', 'disposable.style.email.with+symbol@example.com',
            'fully-qualified-domain@example.com', 'example-indeed@strange-example.com'])
    def test_email_field_ok(self, case):
        self.assertEqual(self.ef.parse_validate(case), case)


    @cases(['', 'sdfsdf@sdsfsdf', 'some_email_email.com'])
    def test_email_field_fail(self, case):
        with self.assertRaises(ValueError) as context:
            self.ef.parse_validate(case)

            self.assertRaises(ValueError, context.exception)

class TestPhoneField(unittest.TestCase):
    def setUp(self):
        self.phf = api.PhoneField(required=False, nullable=True)


    @cases([79175002040, '79175002040'])
    def test_phone_field_ok(self, case):
        self.assertEqual(self.phf.parse_validate(case), case)


    @cases(['', 123213, '791750020404', 791750020403, 791750020])
    def test_phone_field_fail(self, case):
        with self.assertRaises(ValueError) as context:
            self.phf.parse_validate(case)

            self.assertRaises(ValueError, context.exception)

class TestBirthDayField(unittest.TestCase):
    def setUp(self):
        self.bdf = api.BirthDayField(required=False, nullable=True)


    @cases(['01.01.2000', '01.01.1953'])
    def test_birthday_field_ok(self, case):
        self.assertEqual(self.bdf.parse_validate(case), case)


    @cases(['3453.2343', '01.01.1890', 'XXX', 12345])
    def test_birthday_field_faild(self, case):
        with self.assertRaises(ValueError) as context:
            self.bdf.parse_validate(case)

            self.assertRaises(ValueError, context.exception)


class TestSuite(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = None


    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.store)


    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            request_str = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(request_str.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()


    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)


    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "", "arguments": {}},
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "sdd", "arguments": {}},
        {"account": "horns&hoofs", "login": "admin", "method": "online_score", "token": "", "arguments": {}},
    ])
    def test_bad_auth(self, request):
        _, code = self.get_response(request)
        self.assertEqual(api.FORBIDDEN, code)


    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
        {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
        {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    ])
    def test_invalid_method_request(self, request):
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response))


    @mock.patch('store.Store')
    @cases([
        {},
        {"phone": "79175002040"},
        {"phone": "89175002040", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikovotus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": -1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": "1"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.1890"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "XXX"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000", "first_name": 1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "s", "last_name": 2},
        {"phone": "79175002040", "birthday": "01.01.2000", "first_name": "s"},
        {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    ])
    def test_invalid_score_request(self, MockStore, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        mstore = MockStore()
        mstore.get.return_value = ''
        self.store = mstore
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response))


    @mock.patch('store.Store')
    def test_ok_score_admin_request(self, MockStore):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        mstore = MockStore()
        mstore.get.return_value = ''
        self.store = mstore
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)


    @mock.patch('store.Store')
    @cases([
        {},
        {"date": "20.07.2017"},
        {"client_ids": [], "date": "20.07.2017"},
        {"client_ids": {1: 2}, "date": "20.07.2017"},
        {"client_ids": ["1", "2"], "date": "20.07.2017"},
        {"client_ids": [1, 2], "date": "XXX"},
    ])
    def test_invalid_interests_request(self, MockStore, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        mstore = MockStore()
        mstore.get.return_value = ''
        self.store = mstore
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response))

class TestStoreClass(unittest.TestCase):
    def setUp(self):
        self.fake_redis_server = fakeredis.FakeServer()
        self.fake_store = store.Store(use_fake=True, fake_server=self.fake_redis_server)


    def set_key_to_redis(self, case):
        key, val, ex = case.get('key'), case.get('value'), case.get('ex')
        self.store = fakeredis.FakeStrictRedis(server=self.fake_redis_server)
        self.store.set(key, val, ex=ex)


    @cases([
        {'key':'phone', 'value':'79175002040', 'ex':60 * 60},
        {'key':'email', 'value':'vlavrenchenko@mail.ru', 'ex': 60 * 60}
    ])
    def test_invalid_cache_set_max_retry(self, case):
        key, val, ex = case.get('key'), case.get('value'), case.get('ex')
        self.fake_redis_server.connected = False
        self.fake_store.cache_set(key, val, ex)
        self.assertEqual(self.fake_store.get_retry_count(), 5)


    @cases([
        {'key':'phone', 'value':'79175002040', 'ex':60 * 60},
        {'key':'email', 'value':'vlavrenchenko@mail.ru', 'ex':60 * 60}
    ])
    def test_ok_cache_set(self, case):
        key, val, ex = case.get('key'), case.get('value'), case.get('ex')
        self.assertTrue(self.fake_store.cache_set(key, val, ex))
        r = fakeredis.FakeStrictRedis(server=self.fake_redis_server)
        self.assertEqual(r.get(key), val.encode())


    @cases([
        {'key': 'phone'},
        {'key': 'email'},
        {'key': 'last_name'}
    ])
    def test_invalid_cache_get_max_retry(self, case):
        key = case.get('key')
        self.fake_redis_server.connected = False
        store_value = self.fake_store.cache_get(key)
        self.assertEqual(self.fake_store.get_retry_count(), 5)


    @cases([
        {'key':'phone', 'value':'79175002040', 'ex':60 * 60},
        {'key':'email', 'value':'vlavrenchenko@mail.ru', 'ex':60 * 60},
        {'key':'last_name', 'value':'Lavrenchenko', 'ex': 60},
        {'key': 'birthday', 'value':'01.01.1980', 'ex': 60}
    ])
    def test_ok_cache_get(self, case):
        self.set_key_to_redis(case)
        key, val = case.get('key'), case.get('value')
        store_value = self.fake_store.cache_get(key)
        self.assertEqual(store_value, val.encode())


    @cases([
        {'key':'phone'},
        {'key':'email'},
        {'key':'last_name'},
        {'key': 'birthday'}
    ])
    def test_invalid_cache_get(self, case):
        key = case.get('key')
        self.assertFalse(self.fake_store.cache_get(key))


    @cases([
        {'key':'phone', 'value':'79175002040'},
        {'key':'email', 'value':'vlavrenchenko@mail.ru'}
    ])
    def test_invalid_set_max_retry(self, case):
        key, val = case.get('key'), case.get('value')
        self.fake_redis_server.connected = False
        with self.assertRaises(ConnectionError) as context:
            self.fake_store.set(key, val)
            self.assertRaises(ConnectionError, context.exception)

        self.assertEqual(self.fake_store.get_retry_count(), 5)


    @cases([
        {'key':'phone', 'value':'79175002040'},
        {'key':'email', 'value':'vlavrenchenko@mail.ru'}
    ])
    def test_ok_set(self, case):
        key, val = case.get('key'), case.get('value')
        self.assertTrue(self.fake_store.set(key, val))
        r = fakeredis.FakeStrictRedis(server=self.fake_redis_server)
        self.assertEqual(r.get(key), val.encode())


    @cases([
        {'key': 'phone'},
        {'key': 'email'},
        {'key': 'last_name'}
    ])
    def test_invalid_get_max_retry(self, case):
        key = case.get('key')
        self.fake_redis_server.connected = False
        with self.assertRaises(ConnectionError) as context:
            self.fake_store.get(key)
            self.assertRaises(ConnectionError, context.exception)

        self.assertEqual(self.fake_store.get_retry_count(), 5)


    @cases([
        {'key':'phone', 'value':'79175002040'},
        {'key':'email', 'value':'vlavrenchenko@mail.ru'},
        {'key':'last_name', 'value':'Lavrenchenko'},
        {'key': 'birthday', 'value':'01.01.1980'}
    ])
    def test_ok_get(self, case):
        self.set_key_to_redis(case)
        key, val = case.get('key'), case.get('value')
        store_value = self.fake_store.get(key)
        self.assertEqual(store_value, val.encode())

class TestScoringMethods(unittest.TestCase):
    def setUp(self):
        self.fake_redis_server = fakeredis.FakeServer()
        self.fake_store = store.Store(use_fake=True, fake_server=self.fake_redis_server)
        self.r = fakeredis.FakeStrictRedis(server=self.fake_redis_server)

    def tearDown(self):
        pass


    @cases([
        {'phone': '79099934432', 'birthday':'01.01.1990', 'first_name':'Vladimir', 'last_name':'Lavrenchenko',
                        'email':'vlavrenchenko@mail.ru', 'gender':1, 'score':5.0}
    ])
    def test_ok_get_score(self, arguments):
        phone, birthday, first_name, last_name, email, gender, score = arguments
        score = scoring.get_score(self.fake_store, phone=phone, birthday=birthday, email=email, gender=gender,\
                                                                            first_name=first_name, last_name=last_name)

        key = scoring.get_key(first_name, last_name, phone, birthday)
        r = fakeredis.FakeStrictRedis(server=self.fake_redis_server)
        self.assertEqual(float(r.get(key)), score)


    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_get_interests(self, arguments):
        client_ids, date = arguments.get('client_ids') ,arguments.get('date')
        set_iterests(arguments, self.r)

        result_store = {cid: scoring.get_interests(self.fake_store, cid) for cid in client_ids}
        result_fake_r = {cid: json.loads(self.r.get(cid).decode('utf-8')) for cid in client_ids}
        self.assertEqual(result_store, result_fake_r)


    @cases([
        {"client_ids": [4, 5, 6], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [6, 7], "date": "19.07.2017"},
        {"client_ids": [6]},
    ])
    def test_invalid_get_interests(self, arguments):
        client_ids, date = arguments.get('client_ids'), arguments.get('date')
        result_store = {cid: scoring.get_interests(self.fake_store, cid) for cid in client_ids}
        result_fake_r = {cid: [] for cid in client_ids}
        self.assertEqual(result_fake_r, result_store)


if __name__ == "__main__":
    unittest.main()
