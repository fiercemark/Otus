#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import argparse
import json
import datetime
from dateutil import relativedelta
import logging
import hashlib
import uuid
import re
from collections import OrderedDict
from inspect import Parameter, Signature
from optparse import OptionParser
import scoring
import os
# from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from http.server import HTTPServer, BaseHTTPRequestHandler

SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
ADMIN_SCORE = 42
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}

class Field(object):
    def __init__(self, initial_value=None):
        self.initial_value = initial_value

    def validate(self, value):
        return True


class Nullable(Field):
    def __init__(self, *args, initial_value=None, required=False, nullable=False, **kwargs):
        super().__init__(*args, initial_value, **kwargs)
        self.required = required
        self.nullable = nullable

    def validate(self, value):
        if super().validate(value):
            result = ((value is None) or self._validate_nullable(value)) and self._validate_required(value)
            return result
        else:
            return False

    def _validate_nullable(self, value):
        if not self.nullable and not value:
            return False
        return True

    def _validate_required(self, value):
        if self.required and value is None:
            return False
        return True


class Typed(Field):
    ty = (object,)

    def validate(self, value):
        if super().validate(value):
            result = (value is None) or self._validate_value(value)
            return result
        else:
            return False

    def _validate_value(self, value):
        result = False
        for t in self.ty:
            if isinstance(value, t):
                result = result or True
        return result


class Integer(Typed):
    ty = (int,)


class String(Typed):
    ty = (str,)


class IntOrString(Typed):
    ty = (int, str)


class List(Typed):
    ty = (list,)


class Dict(Typed):
    ty = (dict,)


class CharField(String, Nullable):
    pass


class Regex(Field):
    def __init__(self, *args, initial_value=None, pat, **kwargs):
        super().__init__(initial_value)
        self.pat = re.compile(pat)

    def _validate_value_regexp(self, value):
        if not isinstance(value, str):
            value = str(value)
        if self.pat.match(value):
            return True
        return False

    def validate(self, value):
        if super().validate(value):
            result = (value is None) or self._validate_value_regexp(value)
            return result
        else:
            return False


class EmailField(CharField, Regex):
    pass


class PhoneField(IntOrString, Nullable, Regex):
    pass


class Date(Nullable, String, Regex):
    def validate(self, value):
        if super().validate(value):
            return (value is None) or self._validate_date(value)
        else:
            return False

    def _validate_date(self, value):
        try:
            date = datetime.datetime.strptime(value, '%d.%m.%Y')
            current_date = datetime.datetime.now()
        except ValueError as e:
            return False
        else:
            if relativedelta.relativedelta(current_date, date).years > 70:
                return False
        return True


class BirthDayField(Date):
    pass


class GenderField(Nullable, Integer):
    def validate(self, value):
        if super().validate(value):
            return (value is None) or self._validate_gender(value)
        else:
            return False

    def _validate_gender(self, value):
        if not value in [0, 1, 2]:
            return False
        return True


class StructureMeta(type):
    def __new__(mcs, name, bases, attrs):
        current_fields = {}
        for key, value in list(attrs.items()):
            if isinstance(value, Field):
                current_fields[key] = value
                attrs.pop(key)
        attrs['_fields'] = current_fields
        attrs['has'] = []
        attrs['invalid_fields'] = []
        new_class = (super(StructureMeta, mcs).__new__(mcs, name, bases, attrs))
        return new_class


class Structure(metaclass=StructureMeta):
    def __init__(self, json_input=None):
        for name, field in self._fields.items():
            setattr(self, name, field.initial_value)
        self.has = []
        self.invalid_fields = []

        if json_input is not None:
            json_input = json.dumps(json_input)
            json_value = json.loads(json_input)

            if not isinstance(json_value, dict):
                raise RuntimeError("Supplied JSON must be a dictionary")

            for key, value in json_value.items():
                setattr(self, key, value)

    def __setattr__(self, key, value):
        if key in self._fields:
            if self._fields[key].validate(value):
                if value is None:
                    if key in self.invalid_fields:
                        self.invalid_fields.remove(key)
                    if key in self.has:
                        self.has.remove(key)
                else:
                    if key in self.invalid_fields:
                        self.invalid_fields.remove(key)
                    if key not in self.has:
                        self.has.append(key)
                super().__setattr__(key, value)
            else:
                if key not in self.invalid_fields:
                    self.invalid_fields.append(key)
                if key in self.has:
                    self.has.remove(key)


    def validate(self):
        if self.invalid_fields:
            logging.info('Invalid some fields')
            for field in self.invalid_fields:
                logging.info('Invalid value for field {}'.format(field))
            return False
        return True


class ClientIDsField(Nullable, List):
    def validate(self, value):
        if super().validate(value):
            return (value is None) or self._validate_ids(value)
        else:
            return False

    def _validate_ids(self, value):
        for val in value:
            if not isinstance(val, int):
                return False
            return True


class DateField(Date):
    pass


class DateField(Date):
    pass


class ArgumentsField(Dict, Nullable):
    pass

class ClientsInterestsRequest(Structure):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True, pat='^\d{2}.\d{2}.\d{4}$')

    def get_invalid_fields(self):
        return self.invalid_fields

    def get_ids(self):
        return self.client_ids

class OnlineScoreRequest(Structure):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True, pat='^.+@[^\.]+\..+')
    phone = PhoneField(required=False, nullable=True, pat='^7\d{10}$')
    birthday = BirthDayField(required=False, nullable=True, pat='^\d{2}.\d{2}.\d{4}$')
    gender = GenderField(required=False, nullable=True)

    def get_has(self):
        return self.has

    def get_invalid_fields(self):
        return self.invalid_fields

    def validate(self):
        valid_pairs = [('first_name', 'last_name'), ('email', 'phone'), ('birthday', 'gender')]
        if super().validate():
            valid_fields = self.get_has()
            for fv, sv in valid_pairs:
                if fv in valid_fields and sv in valid_fields:
                    return True
            return False
        else:
            return False

class MethodRequest(Structure):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


def check_auth(request):
    if request.is_admin:
        request_str = datetime.datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT
    else:
        request_str = request.account + request.login + SALT
    digest = hashlib.sha512(request_str.encode('utf-8')).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx, store):
    if not request['body']:
        logging.info('No request["body"], error :{}'.format(ERRORS[INVALID_REQUEST]))
        return {'error': ERRORS[INVALID_REQUEST]}, INVALID_REQUEST
    request = MethodRequest(request['body'])
    if not request.validate():
        return {'error': ERRORS[INVALID_REQUEST]}, INVALID_REQUEST

    if not check_auth(request):
        logging.info('Invalid auth, error:{}'.format(ERRORS[FORBIDDEN]))
        return {'error': ERRORS[FORBIDDEN]}, FORBIDDEN

    if request.method == 'online_score':
        logging.info('Request method: {}'.format(request.method))

        score = OnlineScoreRequest(request.arguments)
        if not score.validate():
            responce, code = {'error': {name for name in score.get_invalid_fields()}}, INVALID_REQUEST
            return responce, code

        print('scrore attributes')
        print(type(score.phone))
        print(type(score.email))
        print(type(score.birthday))
        result_score = scoring.get_score(store, score.phone, score.email, score.birthday, score.gender, score.first_name, score.last_name)
        ctx['has'] = score.get_has()

        if request.is_admin:
            logging.info('Admin request, args:{}'.format(request.arguments))
            response = {'score': ADMIN_SCORE}
            code = OK
            return response, code
        response, code = {'score': result_score}, OK
        return response, code

    elif request.method == 'clients_interests':
        logging.info('Request method: {}'.format(request.method))
        result = {}
        interests = ClientsInterestsRequest(request.arguments)
        if not interests.validate():
            response, code = {'error': {name for name in interests.get_invalid_fields()}}, INVALID_REQUEST
            return response, code

        client_ids = interests.get_ids()
        ctx['nclients'] = len(interests.get_ids())

        for id in client_ids:
            result[id] = scoring.get_interests(store, id)

        response, code = result, OK
        return response, code


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }
    store = None

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context, self.store)
                except Exception as e:
                    logging.exception("Unexpected error: {}".format(e))
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return

def setup_logger(log_path):
    log_dir = os.path.split(log_path)[0]
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=log_path, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    setup_logger(opts.log)

    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()