# log analyzer
==============================

This is a simple log analyzer.

Usage example
---------------

1. Run `python log_analyzer.py --config <config_path>`
2. Fill LOG_DIR, REPORT_DIR in the `config.json` using this format
`config = {`
    `'REPORT_SIZE': 1000,`
    `'REPORT_DIR': './reports',`
    `'LOG_DIR': './log'`
`}`

Testing
-------

1. Run the command `python -m unittest -v test_log_analyzer`
2. All test should be `OK`

