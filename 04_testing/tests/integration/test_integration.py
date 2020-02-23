from functools import wraps
import api
import scoring
import os
import redis
import json
import numpy as np
try:
    import mock
except ImportError:
    from unittest import mock

import unittest
import hashlib
import datetime


config = {
    'REDIS_LUNCH_SCRIPT': './tests/integration/start-redis-server.sh'
}

def cases(cases):
    def decorator(f):
        @wraps(f)
        def wrapper(*args):
            for case in cases:
                new_args = args + (case if isinstance(case, tuple) else (case,))
                f(*new_args)
        return wrapper
    return decorator


def lunch_redis():
    print('current path', os.path.dirname(os.path.realpath(__file__)))
    os.system(config.get('REDIS_LUNCH_SCRIPT'))
    return True

class TestIntegrate(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.settings = {'Store': scoring.Store()}
        lunch_redis()
        self.redis = redis.Redis(host='127.0.0.1', port=6379)


    def tearDown(self):
        os.system('pkill redis')
        if os.path.exists('dump.rdb'):
            os.system('rm dump.rdb')


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
                pass
                # print(e)
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
        self.set_online_score_data(arguments, self.redis)
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
        self.set_interests_request_data(arguments, self.redis)
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


if __name__ == "__main__":
    unittest.main()
