import redis
from operator import itemgetter
import json

"""
Connect to local Redis server. StrictRedis complies more closely with standard than
simple Redis client in this package. decode_responses specifies whether or not to convert
bytes to UTF8 character string or treat as raw bytes, e.g. an image, audio stream, etc.
"""
r = redis.StrictRedis(
    host='localhost',
    port=6379,
    charset="utf-8", decode_responses=True)

def add_to_cache(key, value):
    """
    :param key: A valid Redis key string.
    :param value: A Python dictionary to add to cache.
    :return: None
    """
    k = key
    #ut.debug_message("Adding key = ", k)
    #ut.debug_message("Adding data", value)
    r.set(k, value)

def get_from_cache(key):
    """
    :param key: A valid Redis key.
    :return: The "map object" associated with the key.
    """
    result = r.get(key)
    return result

def compute_key(resource, template, fields):
    t = None
    f = None
    if template is not None:
        t = template.items()
        t = tuple(t)
        sorted(t, key=itemgetter(1))
        ts = [str(e[0]) + "=" + str(e[1]) for e in t]
        t = ",".join(ts)
    if fields is not None:
        f = sorted(fields)
        f = "f=" + (",".join(f))
    if f is not None or t is not None:
        result = resource + ":"
    else:
        result = resource
    if t is not None:
        result += t
    if f is not None and t is not None:
        result += "," + f
    elif f is not None and t is None:
        result += f
    return result

def get_keys():
    result = r.keys()
    return result

def save_dict(k, d):
    result = r.hmset(k, d)
    return result

def get_dict(k):
    result = r.hgetall(k)
    return result

def check_query_cache(resource, template, fields):
    key = compute_key(resource, template, fields)
    result = get_from_cache(key)
    if result is not None and len(result)>0:
        result = json.loads(result)
    return result

def add_to_query_cache(resource, template, fields, query_result):
    key = compute_key(resource, template, fields)
    result = add_to_cache(key, json.dumps(query_result))
    return result