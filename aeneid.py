# Ruxin Shen -rs3878
from aeneid.dbservices import dataservice as ds
from flask import Flask
from flask import request
import os
import json
import copy
from aeneid.utils import utils as utils
import re
from aeneid.utils import webutils as wu
from aeneid.dbservices.DataExceptions import DataException
from flask import Response
from urllib.parse import urlencode


# Default delimiter to delineate primary key fields in string.
key_delimiter = "_"


app = Flask(__name__)


def compute_links(result, limit=None, offset=None):
    result['links'] = []

    self = {'rel':"current","href":request.url}
    result['links'].append(self)

    if offset is None:
        offset  = 10
    if limit is not None:
        next_offset = int(offset) + int(limit)
        if int(offset) >= int(limit):
            last_offset = int(offset) - int(limit)
        else:
            last_offset = 0
    else:
        next_offset = offset

    base = request.base_url

    args = {}
    for k,v in request.args.items():
        if not k == "offset":
            args[k] = v
        else:
            args[k] = next_offset

    params = urlencode(args)
    self = {'rel': "next", "href": base + "?" + params}
    result['links'].append(self)


    args = {}
    for k, v in request.args.items():
        if not k == "offset":
            args[k] = v
        else:
            args[k] = last_offset

    params = urlencode(args)
    self = {'rel': "previous", "href": base + "?" + params}
    result['links'].append(self)

    return result

@app.route('/')
def hello_world():
    return """
            You probably want to go either to the content home page or call an API at /api.
            When you despair during completing the homework, remember that
            Audentes fortuna iuvat.
            """

@app.route('/explain', methods=['GET', 'PUT', 'POST', 'DELETE'])
def explain_what():

    result = "Explain what?"
    response = Response(result, status=200, mimetype="text/plain")

    return response

@app.route('/explain/<concept>', methods=['GET', 'PUT', 'POST', 'DELETE'])
def explain(concept):

    mt = "text/plain"

    if concept == "route":
        result = """
                    A route definition has the form /x/y/z.
                    If an element in the definition is of the for <x>,
                    Flask passes the element's value to a parameter x in the function definition.
                    """
    elif concept == 'request':
        result = """
                http://flask.pocoo.org/docs/1.0/api/#incoming-request-data
                explains the request object.
            """
    elif concept == 'method':
        method = request.method

        result = """
                    The @app.route() example shows how to define eligible methods.
                    explains the request object. The Flask framework request.method
                    is how you determine the method sent.
                    
                    This request sent the method:
                    """ \
                    + request.method
    elif concept == 'query':
        result = """
                    A query string is of the form '?param1=value1&param2=value2.'
                    Try invoking ' http://127.0.0.1:5000/explain/query?param1=value1&param2=value2.
                    
                """
        if len(request.args) > 0:
            result += """
                Query parameters are:
                """
            qparams = str(request.args)
            result += qparams
    elif concept == "body":
        if request.method != 'PUT' and request.method != 'POST':
            result = """
                Only PUT and GET have bodies/data.
            """
        else:
            result = """
                The content type was
            """ + request.content_type

            if "text/plain" in request.content_type:
                result += """
                You sent plain text.
                
                request.data will contain the body.
                
                Your plain text was:
                
                """ + str(request.data) + \
                """
                
                Do not worry about the b'' thing. That is Python showing the string encoding.
                """
            elif "application/json" in request.content_type:
                js = request.get_json()
                mt = "application/json"
                result = {
                    "YouSent": "Some JSON. Cool!",
                    "Note": "The cool kids use JSON.",
                    "YourJSONWas": js
                }
                result = json.dumps(result, indent=2)
            else:
                """
                I have no idea what you sent.
                """
    else:
        result = """
            I should not have to explain all of these concepts. You should be able to read the documents.
        """

    response = Response(result, status=200, mimetype=mt)

    return response

@app.route('/api')
def api():
    return 'You probably want to call an API on one of the resources.'

def get_location(dbname, resource_name, k):
    ks = [str(kk) for kk in k.values()]
    ks = "_".join(ks)
    result = "/api/"+dbname+"/"+resource_name+"/"+ks
    return result

def get_context():
    result = {}

    #look for the fields = f1, f2... arg in the query params
    field_list = request.args.get("fields",None)
    if field_list is not None:
        field_list = field_list.split(",")
        result['fields']=field_list

    q_params={}
    for k,v in request.args.items():
        if k not in _predefined_q_params:
            q_params[k]=v

    result['query_params']=q_params

    limit = request.args.get("limit", None)
    result['limit'] = limit
    offset = request.args.get("offset", None)
    result['offset'] = offset
    order_by = request.args.get("order_by", None)
    result['order_by'] = order_by
    children = request.args.get("children", None)
    result['children']= children

    logging.debug("request context = "+ utils.safe_dumps(result))
    return result

def process_links(dbname, resource, primary_key, result):
    pass

@app.route('/api/<dbname>/<resource_name>/<primary_key>', methods=['GET','PUT','DELETE'])
def handle_resource(dbname, resource_name, primary_key):

    resp = Response("Internal server error", status=500, mimetype="text/plain")

    try:

        # The design pattern is that the primary key comes in in the form value1_value2_value3
        key_columns = primary_key.split(key_delimiter)

        # Merge dbname and resource names to form the dbschema.tablename element for the resource.
        # This should probably occur in the data service and not here.
        resource = dbname + "." + resource_name


        if request.method == 'GET':
            # Look for the fields=f1,f2, ... argument in the query parameters.
            field_list = request.args.get('fields', None)
            if field_list is not None:
                field_list = field_list.split(",")

            # Call the data service layer.
            result = ds.get_by_primary_key(resource, key_columns, field_list=field_list)

            if result:
                result = process_result(result, dbname, resource_name, primary_key)
                result = {'data': result}
                result = compute_links(result)
                result_data = json.dumps(result, default=str)
                resp = Response(result_data, status=200, mimetype='application/json')
            else:
                resp = Response("Not found", status=404, mimetype="text/plain")

            if result:
                # We managed to find a row. Return JSON data and 200
                result_data = json.dumps(result, default=str)
                resp = Response(result_data, status=200, mimetype='application/json')
            else:
                resp = Response("NOT FOUND", status=404, mimetype='text/plain')

        elif request.method == 'DELETE':
            result = ds.delete(resource, key_columns)
            if result and result > 0:
                resp = Response("OK", status=200, mimetype='text/plain')
            else:
                resp = Response("NOT FOUND", status=404, mimetype='text/plain')

        elif request.method == 'PUT':
            new_r = request.get_json()
            result = ds.update(resource, key_columns, new_r)
            if result and result == 1:
                resp = Response("OK UPDATED", status=200, mimetype="text/plain")

        else:
            resp = Response("I don't recognize this method", status = 422, mimetype="text/plain")

    except Exception as e:
        # We need a better overall approach to generating correct errors.
        utils.debug_message("Something awful happened, e = ", e)

    return resp

def handle_path(dbname, resource_name, context):
    pass

def query_join(resource, resource2, field_list, tmp, key_columns, resource_name, resource_name2):
    if field_list is None:
        field_list = " * "
    select_statemt = "SELECT " + field_list
    from_statemt = " FROM " + resource+","+resource2

    q = select_statemt+from_statemt

    if tmp is not None:
        where = []
        for k, v in tmp.items():
            temp_s = k + ' = "{0}"'.format(v)
            where.append(temp_s)
        where_ = ' AND '.join(where)
        w_clause = " WHERE " + where_
    else:
        w_clause = ""

    q = q + w_clause

    and_statemt = ""
    tbl = ds.get_data_table(resource)
    tbl2 = ds.get_data_table(resource2)

    f_keys2 = tbl2._get_foreign_key_columns()
    if f_keys2 is not None:
        refer_table2 = tbl2.get_refer_table()
        refer_key2 = tbl2.get_refer_keys()
        if resource in refer_table2:
            and_statemt += " AND "+resource+"."+refer_key2[resource]+"="+resource2+"."+f_keys2[resource]

    f_keys = tbl._get_foreign_key_columns()
    if f_keys is not None:
        refer_table = tbl.get_refer_table()
        refer_key = tbl.get_refer_keys()
        if resource2 in refer_table:
            and_statemt += " AND " + resource2 + "." + refer_key[resource2] + "=" + resource + "." + f_keys[resource2]

    # fk_resource1 = tbl._get_foreign_key_columns()
    # fk_resource2 = tbl2._get_foreign_key_columns()
    # refer_table2 = tbl2.get_refer_table()
    # refer_key2 = tbl2.get_refer_keys()
    # if resource in refer_table2:
    #     for i in refer_key2:
    #         if i in set(fk_resource1):
    #             and_statemt += " AND "+resource+"."+i+" = "+resource2+"."+i

    k = tbl._get_primary_key_columns()
    for i in range(len(k)):
        and_statemt += " AND "+resource+"."+str(k[i])+" = "+ '"{0}"'.format(key_columns[i])

    q = q +and_statemt

    if "WHERE" not in q:
        q = q.replace("AND", "WHERE", 1)

    cursor = tbl._cnx.cursor()
    r = cursor.execute(q)
    r = cursor.fetchall()
    return r


@app.route('/api/<dbname>/<resource_name>/<primary_key>/<resource_name2>', methods=['GET','POST'])
def handle_resource2(dbname, resource_name, primary_key, resource_name2):

    resp = Response("Internal server error", status=500, mimetype="text/plain")

    try:

        # The design pattern is that the primary key comes in in the form value1_value2_value3
        key_columns = primary_key.split(key_delimiter)

        # Merge dbname and resource names to form the dbschema.tablename element for the resource.
        # This should probably occur in the data service and not here.
        resource = dbname + "." + resource_name
        resource2 = dbname + "." + resource_name2


        if request.method == 'GET':
            # Look for the fields=f1,f2, ... argument in the query parameters.
            field_list = request.args.get('fields', None)
            if field_list is not None:
                f_list = field_list.split(",")

            tmp = None
            for k, v in request.args.items():
                if (not k == 'fields') and (not k == 'limit') and (not k == 'offset') and (not k == 'order_by'):
                    if tmp is None:
                        tmp = {}
                    tmp[k] = v

            # Call the data service layer.
            result = query_join(resource, resource2, field_list, tmp, key_columns, resource_name, resource_name2)
            if result:
                #result = process_result(result, dbname, resource_name, primary_key)
                result = {'data': result}
                result = compute_links(result)
                result_data = json.dumps(result, default=str)
                resp = Response(result_data, status=200, mimetype='application/json')
            else:
                resp = Response("Not found", status=404, mimetype="text/plain")

        elif request.method == 'POST':
            new_r = request.get_json()

            p_tbl = ds.get_data_table(resource)
            p_key_columns = p_tbl._get_primary_key_columns()
            tbl2 = ds.get_data_table(resource2)

            #exist = p_tbl.find_by_primary_key(key_columns)
            #if exist >0:

            q = "INSERT INTO "+ resource_name2 + " ( "
            keys = []
            values = []
            for k,v in new_r.items():
                keys.append(k)
                values.append(v)
            f_k = tbl2.get_refer_keys()
            f_k_self = tbl2._get_foreign_key_columns()

            clauses =[]
            for k,v in f_k.items():
                if k == resource:
                    keys = [f_k_self[resource]] + keys
                    for i,va in enumerate(p_key_columns):
                        if va == v:
                            add_key = key_columns[i]
                    clause = "(SELECT "+v +" FROM "+resource+" WHERE "+v + ' = "{0}" ) '.format(add_key)
                    clauses.append(clause)

            key_clause = ",".join(keys) + " ) "
            clauses_ = ",".join(clauses)
            q += key_clause
            q += " VALUES ("
            q += clauses_
            length = len(values)
            for i in range(length):
                q += ',"{0}"'.format(values[i])
            q += ")"

            cursor = p_tbl._cnx.cursor()
            r = cursor.execute(q)
            if r > 0:
                resp = Response("CREATED", status=201, mimetype="text/plain")
            else:
                resp = Response("NOT FOUND IN PARENT TABLE", status=404, mimetype='text/plain')

        # new_r = request.get_json()
            # resource_list1 = {}
            # resource_list2 = {}
            # main_tbl = ds.get_data_table(resource)
            # key_cols = main_tbl._get_primary_key_columns()
            # for i in range(len(key_cols)):
            #     resource_list1[key_cols[i]] = key_columns[i]
            # side_tbl = ds.get_data_table(resource2)
            # # side_key_cols = side_tbl._get_foreign_key_columns()
            # # for i in range(len(side_key_cols)):
            # #     if side_key_cols[i] in set(key_cols):
            # #         resource_list2[side_key_cols[i]] = resource_list1[side_key_cols[i]]
            # refer_keys = side_tbl.get_refer_keys()
            # f_keys = side_tbl._get_foreign_key_columns()
            # for i in range(len(key_cols)):
            #     for k,v in refer_keys.items():
            #         if resource == k  and v == key_cols[i]:
            #             resource_list2[f_keys[k]] = key_columns[i]
            #
            # for k,v in new_r.items():
            #     kk = k.split(".")
            #     if kk[0] == resource_name and kk[1] not in set(key_cols):
            #         resource_list1[k]=v
            #     elif kk[0] == resource_name2:
            #         resource_list2[k]=v
            # result1 = ds.create(resource, resource_list1)
            # result2 = ds.create(resource2, resource_list2)
            # if (result1 + result2) > 1:
            #     resp = Response("CREATED", status=201, mimetype="text/plain")
            # else:
            #     resp = Response("NOT FOUND", status=404, mimetype='text/plain')

        else:
            resp = Response("CAN ONLY GET OR POST", status = 422, mimetype="text/plain")

    except Exception as e:
        # We need a better overall approach to generating correct errors.
        utils.debug_message("Something awful happened, e = ", e)

    #resp['header']=get_context()
    #resp['header']['Location'] = get_location(dbname, resource_name, key_columns)
    return resp

@app.route('/api/<dbname>/<resource_name>', methods = ['GET','POST'])
def handle_collection(dbname, resource_name):

    resp = Response("Internal server error", status=500, mimetype="text/plain")
    result = None
    e = None
    #context = get_context()

    try:

        # Form the compound resource names dbschema.table_name
        #children = context.get("children", None)
        #if children is not None:
            #resp = handle_path(dbname, resource_name, context)
        #else:
        resource = dbname + "." + resource_name
            #tmp = context.get("query_params", None)
            #field_list = context.get("field", None)


        if request.method == 'GET':

            children = request.args.get('children', None)
            if children is None:
                # Get the field list if it exists.
                field_list = request.args.get('fields', None)
                if field_list is not None:
                    field_list = field_list.split(",")

                order_by = request.args.get('order_by', None)
                limit = request.args.get('limit', None)
                offset = request.args.get('offset', None)

                # The query string is of the form ?f1=v1&f2=v2& ...
                # This maps to a query template of the form { "f1" : "v1", ... }
                # We need to ignore the fields parameters.
                tmp = None
                for k,v in request.args.items():
                    if (not k == 'fields') and (not k == 'limit') and (not k == 'offset') and (not k == 'order_by'):
                        if tmp is None:
                            tmp = {}
                        tmp[k] = v

                # Find by template
                result = ds.get_by_template(resource, tmp, field_list=field_list, limit = limit, offset = offset,
                                            order_by = order_by)

                if result:
                    #result = process_result(result[0],dbname,resource_name)
                    result = {'data': result}
                    result = compute_links(result, limit, offset)
                    result_data = json.dumps(result, default=str)
                    resp = Response(result_data, status=200, mimetype='application/json')
                else:
                    resp = Response("Not found", status=404, mimetype="text/plain")
            else:
                result = _get_with_join(dbname, resource_name)
                if result:
                    result_data=json.dumps(result, default=str)
                    resp = Response(result_data, status=200, mimetype='application/json')
                else:
                    resp = Response("Not found", status=404, mimetype="text/plain")

        elif request.method == 'POST':
            new_r = request.get_json()
            result = ds.create(resource, new_r)
            if result and result == 1:
                resp = Response("CREATED", status=201, mimetype="text/plain")

    except Exception as e:
        utils.debug_message("Something awful happened, e = ", e)

    return resp

def process_result(result, dbname, resource_name, primary_key=None):
    ans = {}
    tbl = ds.get_data_table(dbname+"."+resource_name)
    key_cols = tbl._get_primary_key_columns()
    if primary_key is not None:
        for key, value in result.items():
            if key in key_cols:
            #if value == primary_key:
                ans[key] = {"value":value}
                reference = "/api/" + resource_name + "/" + str(value)
                ans[key]['link'] = {'rel':resource_name,'href':reference}
            else:
                ans[key] = value
    else:
        for key, value in result.items():
            ans[key] = {"value": value}
            reference = "/api/" + resource_name + "/" + str(value)
            ans[key]['link'] = {'rel': resource_name, 'href': reference}

    return [ans]

def _get_with_join(dbname, resource_name):
    resoure = dbname +"."+resource_name
    c_list = request.args.get('children', None)
    if c_list is not None:
        children_list = c_list.split(",")

    temp = {}
    to_pop =[]
    for i,v in enumerate(children_list):
        if "=" in v:
            to_pop.append(i)
            a,b = v.split("=")
            temp[a]= b
    for i in range(len(to_pop)):
        children_list.pop(to_pop[len(to_pop)-1-i])

    c_list = ",".join(children_list)

    tmp = {}
    for a,b in temp.items():
        if "." not in a:
            key = resource_name+"."+a
            tmp[key] = b
        else:
            tmp[a]=b

    f_list = request.args.get('fields', None)
    if f_list is not None:
        field_list = f_list.split(",")


    for index,k in enumerate(field_list):
        if "." not in k:
            field_list[index] = resource_name+"."+k
    f_list = ",".join(field_list)

    # f_list = request.args.get('fields', None)
    # if f_list is not None:
    #     field_list = f_list.split(",")

    field_l = []
    for i in field_list:
        tbl_name, attribute = i.split(".")
        field_l.append([tbl_name, attribute])

    order_by = request.args.get('order_by', None)
    limit = request.args.get('limit', None)
    offset = request.args.get('offset', None)

    # The query string is of the form ?f1=v1&f2=v2& ...
    # This maps to a query template of the form { "f1" : "v1", ... }
    # We need to ignore the fields parameters.
    #tmp = None
    for k, v in request.args.items():
        if (not k == 'fields') and (not k == 'limit') and (not k == 'offset') and (not k == 'order_by') and (not k == 'children'):
            if tmp is None:
                tmp = {}
            tmp[k] = v

    select_statement = "SELECT "+f_list
    from_statement = " FROM "+ resource_name+" , "+c_list
    where = []
    for k, v in tmp.items():
        temp_s = k + ' = "{0}"'.format(v)
        where.append(temp_s)
    where_ = ' AND '.join(where)
    w_clause = " WHERE " + where_

    q = select_statement + from_statement + w_clause

    and_statemt=""
    for i in children_list:
        tbl = ds.get_data_table(dbname + "." + i)
        fk_resource = tbl._get_foreign_key_columns()
        ref_t = tbl.get_refer_table()
        ref_k = tbl.get_refer_keys()
        if resoure in set(ref_t):
            and_statemt += " AND "+ resoure+"."+ref_k[resoure] +"=" + i+"."+fk_resource[resoure]

    total = [resource_name] + children_list

    # resource_names = []
    # tbls = []
    # fk_resources = []
    # for i in total:
    #     a = dbname+"."+i
    #     resource_names.append(a)
    #     tbl = ds.get_data_table(dbname+"."+i)
    #     fk_resource = tbl._get_foreign_key_columns()
    #     fk_resources.append(fk_resource)
    #
    # and_statemt = ""
    # for i in range(len(fk_resources)):
    #     for j in range(i+1,len(fk_resources)):
    #         key1 = set(fk_resources[i])
    #         key2 = set(fk_resources[j])
    #         for ii in key1:
    #             if ii in key2:
    #                 and_statemt += " AND " + resource_names[i] + "." + ii + "=" + resource_names[j] + "." + ii

    tbl = ds.get_data_table(dbname + "." + resource_name)
    q = q + and_statemt

    cursor = tbl._cnx.cursor()
    r = cursor.execute(q)
    r = cursor.fetchall()

    answer = []

    ans = {}
    total = [resource_name] + children_list
    for i in total:
        ans[i] = {}

    for i in range(len(r)):
        f_l = field_l.copy()
        for k, v in r[i].items():
            for index, j in enumerate(f_l):
                if k == j[1]:
                    ans[j[0]][k] = v
                    f_l.pop(index)
        answer.append(ans)
        ans = {}
        for i in total:
            ans[i] = {}

    #w_clause = w_clause + " AND appearances.yearid = batting.yearid AND people.playerid = batting.playerid "

    return answer

if __name__ == '__main__':
    app.run()

