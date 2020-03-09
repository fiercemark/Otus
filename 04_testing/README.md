API Testing
==============================

This is a simple api for scoring system.

Usage example
---------------

1. Run `python api.py --port <store port> --log <log_path>`

Testing
-------

**Unit**
1. Run the command `python -m unittest discover -s tests/unit`
2. All 30 tests should be `OK`

**Integration**
1. Export REDIS_SERVER_LUNCHER `export REDIS_SERVER_LUNCHER=./tests/integration/start-redis-server.sh`
2. Run the command `python -m unittest discover -s tests/integration`
3. All 3 tests should be `OK`


