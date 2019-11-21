import unittest
import log_analyzer
from collections import namedtuple
import os
import shutil

class TestBasic(unittest.TestCase):
    def setUp(self):
        config = {
            'REPORT_SIZE': 1000,
            'REPORT_DIR': './reports_test',
            'LOG_DIR': './log_test'
        }
        self.invalid_line = '1.199.4.96 -  - [29/Jun/2017:03:50:22 +0300] "GET /api/v2/slot/4822/groups HTTP/1.1" 200 22 ' \
                         '"-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" ' \
                         '"1498697422-3800516057-4708-9752773" "2a828197ae235b0b3cb"'
        self.correct_line = '1.199.4.96 -  - [29/Jun/2017:03:50:22 +0300] "GET /api/v2/slot/4822/groups HTTP/1.1" 200 22 ' \
                         '"-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" ' \
                         '"1498697422-3800516057-4708-9752773" "2a828197ae235b0b3cb" 0.345'

        self.one_url_statistic = [('/api/v2/banner/25019354', 0.39)] * 10
        self.config = namedtuple('Config', sorted(config))(**config)

    def get_parse_result(self, line):
        return log_analyzer.parserline(line)


    def test_invalid_parser_line(self):
        self.assertEqual(log_analyzer.parserline(''), None, 'Should be None')
        self.assertEqual(self.get_parse_result(self.invalid_line), None, 'Should be None')


    def test_ok_parser_line(self):
        self.assertEqual(self.get_parse_result(self.correct_line), ('/api/v2/slot/4822/groups', 0.345),
                         'Should be tuple of (/api/v2/slot/4822/groups, 0.345')

    def test_ok_calc_statistic(self):
        # statistic_gen = (row for row in self.one_url_statistic)
        statistic_gen = (row for row in self.one_url_statistic)
        self.assertListEqual(list(log_analyzer.cals_statistic(statistic_gen, self.config)),
                             [{'count': 10,
                               'count_perc': 100.0,
                               'time_avg': 0.429,
                               'time_max': 0.39,
                               'time_med': 0.39,
                               'time_perc': 110.0,
                               'time_sum': 4.29,
                               'url': '/api/v2/banner/25019354'}], 'wrong calc statistic')

class TestEnv(unittest.TestCase):
    def setUp(self):
        self.config = namedtuple('Config', sorted(log_analyzer.config))(**log_analyzer.config)
        self.logmeta = namedtuple('logmeta', ['path', 'date', 'expansion'])
        self.log_meta = ''

    def create_reports(self, config):
        if not os.path.exists(config.REPORT_DIR):
            os.makedirs(config.REPORT_DIR)

        files = ['report-2017.07.02.html', 'report-2017.07.03.html', 'report-2017.07.04.html']
        for file in files:
            open(os.path.join(config.REPORT_DIR, file), 'w').close()

    def create_logs(self, config):
        if not os.path.exists(config.LOG_DIR):
            os.makedirs(config.LOG_DIR)

        files = ['nginx-access-ui.log-20170701', 'nginx-access-ui.log-20170702.gz', 'nginx-access-ui.log-20170703.gz']
        for file in files:
            open(os.path.join(config.LOG_DIR, file), 'w').close()


    def test_invalid_find_log(self):
        self.del_logs(self.config)
        last_log = log_analyzer.find_last_log(self.config, self.logmeta)
        self.assertEqual(last_log, None, 'Should be None')


    def del_logs(self, config):
        shutil.rmtree(config.LOG_DIR)


    def test_ok_last_log(self):
        self.create_logs(self.config)
        log_meta = log_analyzer.find_last_log(self.config, self.logmeta)
        self.assertEqual(log_meta.path, './log/nginx-access-ui.log-20170703.gz',
                                            'Should be ./log/nginx-access-ui.log-20170703.gz')

    def del_reports(self, config):
        shutil.rmtree(config.REPORT_DIR)


    def test_ok_find_current_report(self):
        self.create_logs(self.config)
        self.create_reports(self.config)
        log_meta = log_analyzer.find_last_log(self.config, self.logmeta)
        is_report_exist = log_analyzer.check_current_report_done(log_meta, self.config)
        self.assertTrue(is_report_exist, 'Should be True')


class TestFunction(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
