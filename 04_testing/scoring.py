import hashlib
import json
import logging
import time
import random
import api

def get_key(first_name, last_name, phone, birthday):
    key_parts = [
        first_name if first_name and not isinstance(first_name, api.Field) else "",
        last_name if last_name and not isinstance(last_name, api.Field) else "",
        str(phone) if phone and not isinstance(phone, api.Field) else "",
        birthday if birthday and not isinstance(birthday, api.Field) else "",
    ]
    return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()


def get_score(store, phone=None, email=None, birthday=None, gender=None, first_name=None, last_name=None):
    logging.info('Start get_score!')

    key = get_key(first_name, last_name, phone, birthday)
    score = store.cache_get(key) or 0
    if score:
        return score
    if phone:
        score += 1.5
    if email:
        score += 1.5
    if birthday and gender:
        score += 1.5
    if first_name and last_name:
        score += 0.5
    # cache for 60 minutes
    store.cache_set(key, score, 60 * 60)
    return score


def get_interests(store, cid):
    try:
        r = store.get(cid)
    except Exception as e:
        logging.exception('Store unavailable')
    return json.loads(r) if r else []