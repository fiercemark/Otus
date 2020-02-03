from functools import wraps
import api
import scoring

try:
    import mock
except ImportError:
    from unittest import mock


import unittest
import hashlib
import datetime

def cases(cases):
    def decorator(f):
        @wraps(f)
        def wrapper(*args):
            for case in cases:
                new_args = args + (case if isinstance(case, tuple) else (case,))
                f(*new_args)
        return wrapper
    return decorator

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

class TestSuite(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = None

    def get_response(self, request):
        print('self.store', self.store)
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            request_str = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(request_str.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    # def test_empty_request(self):
    #     _, code = self.get_response({})
    #     self.assertEqual(api.INVALID_REQUEST, code)
    #
    # @cases([
    #     {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "", "arguments": {}},
    #     {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "sdd", "arguments": {}},
    #     {"account": "horns&hoofs", "login": "admin", "method": "online_score", "token": "", "arguments": {}},
    # ])
    # def test_bad_auth(self, request):
    #     _, code = self.get_response(request)
    #     self.assertEqual(api.FORBIDDEN, code)
    #
    # @cases([
    #     {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
    #     {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
    #     {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    # ])
    # def test_invalid_method_request(self, request):
    #     print('', end='\n')
    #     print('request', request)
    #     self.set_valid_auth(request)
    #     response, code = self.get_response(request)
    #     self.assertEqual(api.INVALID_REQUEST, code)
    #     self.assertTrue(len(response))
    #
    # @cases([
    #     {},
    #     {"phone": "79175002040"},
    #     {"phone": "89175002040", "email": "stupnikov@otus.ru"},
    #     {"phone": "79175002040", "email": "stupnikovotus.ru"},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": -1},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": "1"},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.1890"},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "XXX"},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000", "first_name": 1},
    #     {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
    #      "first_name": "s", "last_name": 2},
    #     {"phone": "79175002040", "birthday": "01.01.2000", "first_name": "s"},
    #     {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    # ])
    # def test_invalid_score_request(self, arguments):
    #     request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
    #     self.set_valid_auth(request)
    #     response, code = self.get_response(request)
    #     self.assertEqual(api.INVALID_REQUEST, code, arguments)
    #     self.assertTrue(len(response))


    @mock.patch('scoring.Store')
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
    def test_ok_score_request(self, MockStore, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        print('MockStore', MockStore)
        print('arguments', arguments)
        mstore = MockStore()
        mstore.cache_get.return_value = ''
        mstore.cache_set.return_value = True
        self.store = mstore
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0, arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

        key_parts = [
            arguments.get('first_name', ''),
            arguments.get('last_name', ''),
            str(arguments.get('phone', '')),
            arguments.get('birthday', ''),
        ]
        print('key_parts', key_parts)
        key = scoring.get_key(key_parts)
        print('score', score)
        print('mstore.cache_set.call_args', mstore.cache_set.call_args)
        print('mstore.cache_get.called', mstore.cache_get.called)
        print('mstore.cache_set.called', mstore.cache_set.called)
        mstore.cache_get.assert_called()
        mstore.cache_set.assert_called()
        print('mstore.cache_get.call_args', mstore.cache_get.call_args)
        print('key', key)
        mstore.cache_get.assert_called_with(key)
        mstore.cache_set.assert_called_with(key, score, 3600)



    # def test_ok_score_admin_request(self):
    #     arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
    #     request = {"account": "horns&hoofs", "login": "admin", "method": "online_score", "arguments": arguments}
    #     self.set_valid_auth(request)
    #     self.store = MockStore()
    #     response, code = self.get_response(request)
    #     self.assertEqual(api.OK, code)
    #     score = response.get("score")
    #     self.assertEqual(score, 42)
    #
    # @cases([
    #     {},
    #     {"date": "20.07.2017"},
    #     {"client_ids": [], "date": "20.07.2017"},
    #     {"client_ids": {1: 2}, "date": "20.07.2017"},
    #     {"client_ids": ["1", "2"], "date": "20.07.2017"},
    #     {"client_ids": [1, 2], "date": "XXX"},
    # ])
    # def test_invalid_interests_request(self, arguments):
    #     request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
    #     self.set_valid_auth(request)
    #     response, code = self.get_response(request)
    #     self.assertEqual(api.INVALID_REQUEST, code, arguments)
    #     self.assertTrue(len(response))
    #
    # @cases([
    #     {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
    #     {"client_ids": [1, 2], "date": "19.07.2017"},
    #     {"client_ids": [0]},
    # ])
    # def test_ok_interests_request(self, arguments):
    #     request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
    #     self.set_valid_auth(request)
    #     response, code = self.get_response(request)
    #     self.assertEqual(api.OK, code, arguments)
    #     self.assertEqual(len(arguments["client_ids"]), len(response))
    #     self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v)
    #                     for v in response.values()))
    #     self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]))


if __name__ == "__main__":
    unittest.main()

