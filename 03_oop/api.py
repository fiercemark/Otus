#!/usr/bin/env python
# -*- coding: utf-8 -*-
import abc
import json
import datetime
from dateutil import relativedelta
import logging
import hashlib
import uuid
from optparse import OptionParser
import re
from http.server import HTTPServer, BaseHTTPRequestHandler

import scoring

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
    def __init__(self, nullable=False, required=False):
        self.nullable = nullable
        self.required = required

    def parse_validate(self, value):
        if self.required and not self.nullable and not value:
            raise ValueError('value must be defined')


class CharField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        if isinstance(value, str):
            return value
        raise ValueError("value is not a string")


class ArgumentsField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        if isinstance(value, dict):
            return value
        raise ValueError("value in not a dict")


class EmailField(CharField):
    def parse_validate(self, value):
        super().parse_validate(value)
        pat = re.compile('^.+@[^\.]+\..+')
        if pat.match(value):
            return value
        raise ValueError("value is not email")


class PhoneField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        pat = re.compile('^7\d{10}$')
        if isinstance(value, int):
            value = str(value)
        if pat.match(value):
            return value
        raise ValueError("value is not phone")


class DateField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        pat = re.compile('^\d{2}.\d{2}.\d{4}$')
        if pat.match(value):
            try:
                date = datetime.datetime.strptime(value, '%d.%m.%Y')
            except ValueError:
                raise ValueError('Date value is not a date')
            else:
                return date
        else:
            raise ValueError('Date value is not a date')


class BirthDayField(DateField):
    def parse_validate(self, value):
        date = super().parse_validate(value)

        current_date = datetime.datetime.now()
        if relativedelta.relativedelta(current_date, date).years < 70:
            return value
        raise ValueError('Date value older than 70 years')


class GenderField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        if value in [0, 1, 2]:
            return value
        raise ValueError('value is not gender')


class ClientIDsField(Field):
    def parse_validate(self, value):
        super().parse_validate(value)
        if not isinstance(value, list):
            raise ValueError('value should be array')
        for val in value:
            if not isinstance(val, int):
                raise ValueError('value is not ClientIDsField')
        return value


class RequestHandler(object):
    def validate_handle(self, request, arguments, ctx, store):
        if not arguments.is_valid():
            return arguments.errfmt(), INVALID_REQUEST
        return self.handle(request, arguments, ctx, store)

    def handle(self, request, arguments, ctx, store):
        return {}, OK


class RequestMeta(type):
    def __new__(mcs, name, bases, attrs):
        field_list = []
        for attr, value in attrs.items():
            if isinstance(value, Field):
                field_list.append((attr, value))
        cls = super().__new__(mcs, name, bases, attrs)
        cls.fields = field_list
        return cls


class Request(metaclass=RequestMeta):
    def __init__(self, request):
        self.errors = []
        self.request = request
        self.is_cleaned = False

    def clean(self):
        for attr, value in self.fields:
            if value.required:
                if attr not in self.request:
                    self.errors.append((attr, 'value required.'))
                    continue
            if attr in self.request:
                try:
                    val = self.request.get(attr)
                    value.parse_validate(val)
                    setattr(self, attr, val)
                except ValueError as ex:
                    self.errors.append((attr, ex))
        self.is_cleaned = True

    def is_valid(self):
        if not self.is_cleaned:
            self.clean()
        return not self.errors

    def errfmt(self):
        return ", ".join([attr for attr, ex in self.errors])


class ClientsInterestsRequest(Request):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True)


class ClientsInterestsHandler(RequestHandler):
    request_type = ClientsInterestsRequest

    def handle(self, request, arguments, ctx, store):
        ctx["nclients"] = len(arguments.client_ids)
        return {cid: scoring.get_interests(store, cid) for cid in arguments.client_ids}, OK


class OnlineScoreRequest(Request):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def pair_is_valid(self):
        valid_pairs = [('first_name', 'last_name'), ('email', 'phone'), ('birthday', 'gender')]
        valid = False
        if super().is_valid():
            for fv, sv in valid_pairs:
                if self.__dict__.get(fv) not in ['', None] and self.__dict__.get(sv) not in ['', None] :
                    valid = True
        return valid

class OnlineScoreHandler(RequestHandler):
    request_type = OnlineScoreRequest

    def handle(self, request, arguments, ctx, store):
        ctx['has'] = [key for key, val in arguments.fields if key in arguments.__dict__]
        if not arguments.pair_is_valid():
            logging.info('No Valid Paired Arguments, code:{}'.format(INVALID_REQUEST))
            return ERRORS[INVALID_REQUEST], INVALID_REQUEST
        if request.is_admin:
            logging.info('Use admin Auth')
            score = ADMIN_SCORE
        else:
            score = scoring.get_score(store, arguments.phone, arguments.email, arguments.birthday,
                                         arguments.gender, arguments.first_name, arguments.last_name)

        return {"score": score}, OK


class MethodRequest(Request):
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
    methods_map = {
        "online_score": OnlineScoreHandler,
        "clients_interests": ClientsInterestsHandler,
    }
    method_request = MethodRequest(request["body"])
    if not method_request.is_valid():
        logging.info('Invalid Method Request, code:{}, invalid fields:{}'.format(INVALID_REQUEST, method_request.errfmt()))
        return method_request.errfmt(), INVALID_REQUEST
    if not check_auth(method_request):
        logging.info('Invalid Auth, code:{}'.format(FORBIDDEN))
        return None, FORBIDDEN

    handler_cls = methods_map.get(method_request.method)
    if not handler_cls:
        logging.info('Method Not Found, code:{}'.format(NOT_FOUND))
        return "Method Not Found", NOT_FOUND

    response, code = handler_cls().validate_handle(method_request,
                                                   handler_cls.request_type(method_request.arguments),
                                                   ctx, store)
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
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            # @TODO: return errors as array
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
