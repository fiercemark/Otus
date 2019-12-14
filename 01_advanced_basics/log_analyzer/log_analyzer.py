#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from collections import namedtuple, defaultdict
from datetime import datetime
import gzip
import json
import logging
from statistics import median
import os
import re

# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

config = {
    'REPORT_SIZE': 1000,
    'REPORT_DIR': './reports',
    'LOG_DIR': './log'
}

REPORT_TEMPLATE_PATH =  "./template.html"

LOG_NAME_RE = re.compile(r'(?P<name>^nginx-access-ui\.log-(?P<date>[0-9]+)?(?P<extension>\.gz)?)?$')

LOG_ROW_RE = re.compile(r'(^\S+ )\S+\s+\S+ (\[\S+ \S+\] )' 
                r'(\"\S+ (\S+) \S+\") \d+ \d+ \"\S+\" ' 
                r'\".*\" \"\S+\" \"\S+\" \"\S+\" (\d+\.\d+)')

LogMeta = namedtuple('LogMeta', ['path', 'date', 'expansion'])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config_path', help='config path', default='/usr/local/etc/config.json')
    return parser.parse_args()


def create_logger(log_path):
    formatter = logging.Formatter('[%(asctime)s] %(levelname).1s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('log_analyzer')

    if log_path:
        log_dir = os.path.split(log_path)[0]
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

    log_hadler = logging.FileHandler(log_path) if log_path else logging.StreamHandler()
    log_hadler.setFormatter(formatter)
    logger.addHandler(log_hadler)
    logger.setLevel(logging.INFO)
    return logger


def merge_config(default_config, file_config):
    result = {}

    default_keys = default_config.keys()
    file_config_keys = file_config.keys()

    for k in set(list(default_keys) + list(file_config_keys)):
        if k in file_config:
            result[k] = file_config[k]
        else:
            result[k] = default_config[k]

    return result


def find_last_log(config, LogMeta):
    last_file = None
    last_file_date = None

    if not os.path.exists(config.LOG_DIR):
        return None

    for file_name in os.listdir(config.LOG_DIR):
        match = LOG_NAME_RE.match(file_name)
        if not match:
            continue
        date_parse = match.groupdict()['date']
        file = match.groupdict()['name']
        try:
            date = datetime.strptime(date_parse, '%Y%m%d')
        except ValueError:
            logging.exception('Wrong date in log file', exc_info=True)
            continue

        if not last_file or date > last_file_date:
            last_file = file
            last_file_date = date

    if not last_file:
        return None

    if last_file.split('.')[-1] == 'gz':
        expansion = '.gz'
    else:
        expansion = ''

    return LogMeta(path=os.path.join(config.LOG_DIR, last_file), date=datetime.strftime(last_file_date,'%Y%m%d'),
                                            expansion=expansion)


def generate_report_name(log_meta):
    return 'report-' + '.'.join([log_meta.date[:4], log_meta.date[4:6], log_meta.date[6:]]) + '.html'


def check_current_report_done(log_meta, config):
    report_name = generate_report_name(log_meta)
    if report_name in os.listdir(config.REPORT_DIR):
        return True
    return False


def parserline(line):
    if not LOG_ROW_RE.match(line):
        return None
    url_path = LOG_ROW_RE.match(line).group(4)
    request_time = float(LOG_ROW_RE.match(line).group(5))
    result = (url_path, request_time)
    return result


def xreadlines(log_meta, logger, parser=parserline, errors_limit=None):
    total_lines = 0
    processed = 0
    error = 0
    opener = gzip.open(log_meta.path, 'rb') \
                    if log_meta.expansion == '.gz' \
                    else open(log_meta.path, 'rb')
    with opener as log:
        for line in log:
            total_lines += 1
            line = line.decode('utf-8')
            parsed_line = parser(line)
            if not parsed_line:
                error += 1
                continue

            processed += 1
            yield parsed_line

    if errors_limit is not None and total_lines > 0 and error / float(total_lines) > errors_limit:
        raise RuntimeError('To much errors in log!')


def update_statistic_store(store, url, response_time):
    rec = store.get(url)
    if not rec:
        rec = {
            'url': url,
            'request_count': 0,
            'response_time_sum': response_time,
            'max_response_time': response_time,
            'avg_responce_time': 0.,
            'all_responce_time': []
        }
        store[url] = rec

    rec['request_count'] += 1
    rec['response_time_sum'] += response_time
    rec['max_response_time'] = max(store[url]['max_response_time'], response_time)
    rec['avg_responce_time'] = rec['response_time_sum'] / rec['request_count']
    rec['all_responce_time'].append(response_time)


def cals_statistic(log_lines, config_meta):
    url_count = 0
    total_req_time = 0.0
    store = {}
    for url, request_time in log_lines:
        url_count += 1
        total_req_time += request_time
        update_statistic_store(store, url, request_time)
    agreggatebyurl = sorted(store.items(), key=lambda item : item[1]['avg_responce_time'], reverse=True)
    if config_meta.REPORT_SIZE > len(agreggatebyurl):
        agreggatebyurl = agreggatebyurl[:config_meta.REPORT_SIZE]

    #
    for url, val in agreggatebyurl:
        yield  {
                'url': url,
                'count': val['request_count'],
                'count_perc': round((val['request_count'] / url_count) * 100.0, 5) ,
                'time_sum': round(val['response_time_sum'], 5),
                'time_med': round(median(val['all_responce_time']), 5),
                'time_perc': round((val['response_time_sum'] / total_req_time) * 100.0, 5),
                'time_max': round(val['max_response_time'], 5),
                'time_avg': round(val['response_time_sum'] / val['request_count'], 5)
        }


def generate_report(statistic, config, log_meta, template_path):
    with open(template_path, 'rb') as tf:
        template_file = tf.read().decode('utf-8')

    result = template_file.replace("{", "{{").replace("}", "}}").replace("{table_json}", "table_json").\
                                                                format(table_json=list(statistic))
    report_name = generate_report_name(log_meta)

    if not os.path.exists(config.REPORT_DIR):
        os.makedirs(config.REPORT_DIR)

    with open(os.path.join(config.REPORT_DIR, report_name), 'wb') as fw:
        result = result.encode('utf-8')
        fw.write(result)


def main(config, logger):
    LogMeta = namedtuple('LogMeta', ['path', 'date', 'expansion'])
    log_meta = find_last_log(config, LogMeta)
    if not log_meta:
        logger.info('Sorry. No logs found!!!!')
        return
    logger.info('Find last log file: {}'.format(log_meta.path))

    logger.info('Checking the report')
    if os.path.exists(config.REPORT_DIR) and os.listdir(config.REPORT_DIR):
        exist = check_current_report_done(log_meta, config)
        if exist:
            logger.info('The report has already been generated')
            return

    logger.info('Start reading the log')
    try:
        log_lines_it = xreadlines(log_meta, logger, parser=parserline)
    except RuntimeError as e:
        logger.exception('msg: {}'.format(e), exc_info=True)

    staticticit = cals_statistic(log_lines_it, config)
    logger.info('Statistics calculation is finished')
    generate_report(staticticit, config, log_meta, REPORT_TEMPLATE_PATH)
    logger.info('Calculation generation is finished')


if __name__ == "__main__":
    args = parse_args()

    if args.config_path:
        with open(args.config_path, 'rb') as conf:
            ext_config = json.load(conf, encoding='utf-8')

    config.update(ext_config)

    logger = create_logger(config.get('SCRIPT_LOG_PATH'))
    logger.info('Analyzer start work')

    Config = namedtuple('Config', sorted(config))
    config = Config(**config)

    try:
        main(config, logger)
    except:
        logger.exception('Something wrong')