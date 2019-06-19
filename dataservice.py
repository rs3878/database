import pymysql.cursors
import json
import copy
import redis_cache.data_cache as dc


db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.


def set_config():
    """
    Creates the DB connection and sets the global variables.

    :param cfg: Application configuration data.
    :return: None
    """
    global db_schema
    global cnx

    db_params = {
        "dbhost": "localhost",
        "port": 3306,
        "dbname": "lahman2017",
        "dbuser": "dbuser",
        "dbpw": "dbuserdbuser",
        "cursorClass": pymysql.cursors.DictCursor,
        "charset": 'utf8mb4'
    }

    db_schema = "lahman2017"

    cnx = get_new_connection(db_params)

# Given one of our magic templates, forms a WHERE clause.
# { a: b, c: d } --> WHERE a=b and c=d. Currently treats everything as a string.
# We can fix this by using PyMySQL connector query templates.
def templateToWhereClause(t):
    s = ""
    for k,v in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s;

    return s

def run_q(cnx, q, args, fetch=False, commit=True):
    """
    :param cnx: The database connection to use.
    :param q: The query string to run.
    :param args: Parameters to insert into query template if q is a template.
    :param fetch: True if this query produces a result and the function should perform and return fetchall()
    :return:
    """
    #debug_message("run_q: q = " + q)
    #ut.debug_message("Q = " + q)
    #ut.debug_message("Args = ", args)

    result = None

    try:
        cursor = cnx.cursor()
        result = cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        if commit:
            cnx.commit()
    except pymysql_exceptions as original_e:
        #print("dffutils.run_q got exception = ", original_e)
        raise(original_e)

    return result

def retrieve_by_template(table, t, fields=None, limit=None, offset=None, orderBy=None, use_cache=False):

    if use_cache:
        result = check_cache(table, t, fields, limit, offset, orderBy)
        print("\nCheck cache returned ", result, "\n")
        if result is not None and len(result) > 0:
            print("CACHE HIT")
            return result
        else:
            print("CACHE MISS")

    original_fields = fields

    if t is not None:
        w = templateToWhereClause(t)
    else:
        w = ""

    if orderBy is not None:
        o = "order by " + ",".join(orderBy['fields']) + " " + orderBy['direction'] + " "
    else:
        o = ""

    if limit is not None:
        w += " LIMIT " + str(limit)
    if offset is not None:
        w += " OFFSET " + str(offset)

    if fields is None:
        fields = " * "
    else:
        fields = " " + ",".join(fields) + " "

    #cursor=cnx.cursor()
    q = "SELECT " + fields + " FROM " + table + " " + w + ";"

    cnx = get_new_connection()
    r = run_q(cnx, q, None, fetch=True, commit=True)

    if use_cache and r is not None and len(r)>0:
        add_to_cache(table, t, original_fields, limit, offset, orderBy, r)

    return r

default_db_params = {
    "dbhost": "localhost",                    # Changeable defaults in constructor
    "port": 3306,
    "dbname": "lahman2017",
    "dbuser": "dbuser",
    "dbpw": "dbuserdbuser",
    "cursorClass": pymysql.cursors.DictCursor,        # Default setting for DB connections
    "charset":  'utf8mb4'                             # Do not change
}
def get_new_connection(params=default_db_params):
    cnx = pymysql.connect(
        host=params["dbhost"],
        port=params["port"],
        user=params["dbuser"],
        password=params["dbpw"],
        db=params["dbname"],
        charset=params["charset"],
        cursorclass=params["cursorClass"])
    return cnx

def check_cache(table, tmp, fields, limit, offset, orderBy):

    cache_tmp = copy.copy(tmp)
    if limit:
        cache_tmp['limit'] = limit
    if offset:
        cache_tmp['offset'] = offset
    if orderBy:
        cache_tmp['orderBy'] = orderBy

    result = dc.check_query_cache(table, cache_tmp, fields)
    return result


def add_to_cache(table, tmp, fields, limit, offset, orderBy, q_result):
    cache_tmp = copy.copy(tmp)
    if limit:
        cache_tmp['limit'] = limit
    if offset:
        cache_tmp['offset'] = offset
    if orderBy:
        cache_tmp['orderBy'] = orderBy

    result = dc.add_to_query_cache(table, cache_tmp, fields, q_result)
    return result
