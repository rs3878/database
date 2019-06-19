# Your implementation goes in this file.
# Import package to enable defining abstract classes in Python.
# Do not worry about understanding abstract base classes. This is just a class that defines
# some methods that subclasses must implement.

from src.BaseDataTable import BaseDataTable
import csv
import copy
import json
import logging

class Index():
    def __init__(self, name = None, table = None, columns = None, kind = None, loadit = None):
        if loadit:
            logging.debug("Loading an index.")
            self.from_json(table, loadit)
            logging.debug("Loaded index. name = %s, columns = %s, kind = %s, table = %s",
                          self.name, str(self.columns), self.kind, self.table.get_table_name())
        else:
            logging.debug("Creating index. name = %s, columns =%s, kind = %s, table = %s",
                          name, str(columns), kind, table.get_table_name())
            columns.sort()
            self.name = name
            self.columns = columns
            self.kind = kind

            self._index_data = None
            self.table = table
            self.table_name = table.get_table_name()
    def __str__(self):
        result = str(type(self)) + ": name = " + str(self.name)
        result += "\ntable =  " + str(self.table.get_table_name())

        if self.columns is not None:
            result += "\ncolumn names = " + str(self.columns)
        if self.kind is not None:
            result += "\nkind = " + str(self.kind)

        if self._index_data is not None:
            result += "\n\nGonna print out first 5 index_keys and 5 rows each if they have more than 5\n"
            to_print = min(5, len(self._index_data))
            i = 0
            for k,v in self._index_data.items():
                if i >= to_print:
                    break
                else:
                    to_print2 = min(5, len(v))
                    result += "\n index_key "+str(k)
                    j = 0
                    for k2,v2 in v.items():
                        if j >= to_print2:
                            break
                        else:
                            result += "\nrid: "+str(k2)+" ;row:"+str(v2)
                            j +=1
                    i +=1
        return result

    def compute_index_value(self, row):
        key_v = [str(row[k]) for k in self.columns]
        key_v = "_".join(key_v)
        return key_v
    def matches_index(self, template):
        k = set(list(template.keys()))
        c = set(self.columns)

        if c.issubset(k):
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None
        return kk

    def add_to_index(self, row, rid):
        index_key = self.compute_index_value(row)
        if self._index_data is None:
            self._index_data = {}

        bucket = self._index_data.get(index_key, None)
        if self.kind != "INDEX" and bucket is not None:
            raise KeyError("Duplicate key, index is "+self.name+", table is "+ self.table_name+\
                           ", key = "+ index_key)
        else:
            if bucket is None:
                bucket = {}

            bucket[rid] = row
            self._index_data[index_key] = bucket
    def delete_from_index(self, row, rid):
        self.remove_from_index(row, rid)
    def remove_from_index(self, row, rid):
        index_key = self.compute_index_value(row)
        if self._index_data is None:
            raise KeyError("Nothing to delete")

        new_bucket = {}
        for k,v in self._index_data.items():
            if k != index_key:
                new_bucket[k] = v

        new_bucket[index_key] = {}
        for k,v in self._index_data[index_key].items():
            if k != rid:
                new_bucket[index_key][k] = v
        new_bucket = { k : v for k,v in new_bucket.items() if v}

        self._index_data=new_bucket

    def _build(self):
        pass

    def to_json(self):
        """
        converts index data and state to a json object
        :return: json representation
        """
        result = {}
        result['name'] = self.name
        result['columns'] = self.columns
        result['kind'] = self.kind
        result['table_name'] = self.table_name
        result['index_data'] = self._index_data
        return result
    def from_json(self, table, loadit):
        self.name = loadit['name']
        self.columns = loadit['columns']
        self.kind = loadit['kind']
        self.table = table
        self._index_data = loadit['index_data']
        self.table_name = table.get_table_name()
        return self

    def find_rows(self, tmp):
        t_keys = tmp.keys()
        t_vals = [str(tmp[k]) for k in self.columns]
        t_s = "_".join(t_vals)

        d = self._index_data.get(t_s, None)
        if d is not None:
            d = list(d.keys())
        return d


    def get_no_of_entries(self):
        return len(self._index_data.keys())

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, column_names = None, primary_key_columns=None, loadit=False):
        """
        :param table_name: Name of the table. This is the table name for an RDB table or the file name for
            a CSV file holding data.
        :param primary_key_columns: List, in order, of the columns (fields) that comprise the primary key.
            A primary key is a set of columns whose values are unique and uniquely identify a row. For Appearances,
            the columns are ['playerID', 'teamID', 'yearID']
        """
        self._table_name = table_name
        self._column_names = column_names
        self._primary_key_columns = primary_key_columns

        #??
        self._rows = None
        self._default_directory = None

        self._indexes = None

        if not loadit:
            if column_names is None or table_name is None:
                raise ValueError("Please provide more info")
            self._next_row_id = 1
            self._rows = {}
            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")
    def __str__(self):
        result = str(type(self)) + ": name = " + str(self._table_name)
        result += "\nkey_columns =  " + str(self._primary_key_columns)

        if self._column_names is not None:
            result += "\ncolumn names = " + str(self._column_names)
        if self._rows is not None:
            row_count = len(self._rows)
        else:
            row_count = 0
        result += "\nnumber of rows = " + str(row_count)
        to_print = min(5, row_count)
        i = 0
        if self._rows is not None:
            for k, v in self._rows.items():
                if i > to_print:
                    break
                else:
                    result += "\n" + str(k) + str(v)
                    i += 1

        return result

    def get_table_name(self):
        return self._table_name
    def get_primary_key(self):
        return self._primary_key_columns

    #old methods
    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        waiting_to_be_appended = []

        for r in self._rows:
            if self.matches_template(template, r):
                new_r = self._update_row(r, new_values)
                new_k = self._get_key(new_r)
                self._rows.remove(r)
                k = self.find_by_primary_key(new_k)
                if k is not None:
                    self.add_row(r)
                    raise ValueError('ick')
                else:
                    waiting_to_be_appended.append(new_r)
        for wtv in waiting_to_be_appended:
            self.add_row(wtv)
        return len(waiting_to_be_appended)
    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        result = 0
        for tup in key_fields:
            tmp = {}
            for kc in self._key_columns:
                tmp[kc] = tup
            result += self.update_by_template(tmp, new_values)
        return result
    def add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)
    def insert_old(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        k = self._get_key(new_record)
        test_it = self.find_by_primary_key(k)

        if test_it is not None:
            raise ValueError("What part of unique is not clear?")
        else:
            self.add_row(new_record)
    def _project_old(self, rows, field_list):
        if field_list is None:
            return rows
        if rows is None:
            return None

        new_rows = []
        for r in rows:
            new_r = {f:r[f] for f in field_list}
            new_rows.append(new_r)

        return new_rows

    def find_by_template(self, tmp, fields, use_index = True):

        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index is False:
            result = self.find_by_scan_template(tmp, self.get_rows())

        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        final_result = self._project_dict(result, fields)

        new_t = CSVDataTable(table_name="Derived:"+self._table_name,loadit=True)
        new_t.load_from_rows(table_name="Derived:"+self._table_name,rows=final_result)
        return new_t

    def find_by_primary_key(self, key_fields, field_list=None, use_index = True):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """
        tmp = dict(zip(self._primary_key_columns, key_fields))
        result = self.find_by_template(tmp, field_list, use_index=use_index)
        rows = result.get_rows()
        if rows:
            for k,v in rows.items():
                return v
        else:
            return None

    def delete_by_template(self, tmp, use_index=True):
        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index is False:
            result = self.find_by_scan_template(tmp, self._rows)
        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        count = 0
        for k, v in result.items():
            self._remove_row(k)
            count += 1

        """
        new_rows = []
        count = 0
        for r in self._rows:
            if not self.matches_template(template,r):
                new_rows.append(copy.copy(r))
            else:
                count += 1
        self._rows = new_rows
        """
        return count

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        tmp = dict(zip(self._primary_key_columns, key_fields))
        result = self.delete_by_template(tmp)
        return result

    def get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id
    def get_row_with_rid(self, rid):
        return self._rows[rid]
    def get_rows(self):
        return self._rows

    def build(self, i_name):
        idx = self._indexes[i_name]
        for k,v in self._rows.items():
            idx.add_to_index(row = v,rid = k)
    def get_best_index(self, t):
        best = None
        n = None

        if self._indexes is not None:
            for k, v in self._indexes.items():
                cnt = v.matches_index(t)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k
                    else:
                        if cnt > best:
                            best = len(v.keys())  # len(v.keys) is just cnt right?
                            n = k
        return n
    def find_by_index(self, tmp, idx):
        r = idx.find_rows(tmp) #find rids that matches the idx
        if r is not None:
            result = [self._rows[k] for k in r]
            return result
        else:
            return None

    def matches_template(self, tmp, row):
        if tmp is None:
            return True
        keys = tmp.keys()
        for k in keys:
            # if key is there, give me the value, o/w, return None
            v = row.get(k, None)
            if tmp[k] != v:
                return False
        return True
    def find_by_scan_template(self, template, rows):
        some_rows = {}
        for k,r in self._rows.items():
            if self.matches_template(template, r):
                some_rows[k] = copy.copy(r)
        return some_rows

    def _project_dict(self, rows, fields):
        if fields is None:
            return rows
        if rows is None:
            return None
        new_rows = {}
        for k, r in rows.items():
            new_rows[k]={}
        for k,r in rows.items():
            for rk,rv in r.items():
                if rk in fields:
                    new_rows[k][rk] = rv
        return new_rows
    def _get_key(self, row):
        result = [row[k] for k in self._key_columns]
        return result
    def _get_sub_template(self, temp, table_name):
        pass

    def _add_row(self, r):
        if self._rows is None:
            self._rows = {}

        rid = self.get_next_row_id()
        self._rows[rid] = copy.copy(r)

        if self._indexes is not None:
            for n, idx in self._indexes.items():
                idx.add_to_index(r, rid)
    def insert(self, new_record):
        self._add_row(new_record)
    def _remove_row(self, rid):
        for n, idx in self._indexes.items():
            idx.remove_from_index(self._rows[rid],rid)
        #del[self._indexes[rid]] #should it be self._rows????
        del[self._rows[rid]]

    def save(self):
        d = {
            "state":{
                "table_name":self._table_name,
                "primary_key_columns":self._primary_key_columns,
                "next_rid":self.get_next_row_id(),
                "column_names":self._column_names
            }
        }

        #fn = CSVDataTable._default_directory +self._table_name+".json"
        fn = self._table_name+".json"
        d['rows'] = self._rows

        for k,v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d['indexes'] = idxs

        d = json.dumps(d, indent = 2)
        with open(fn, "w+") as outfile:
            outfile.write(d)
        return outfile


        return self._rows
    def load(self):
        #fn = CSVDataTable._default_directory + self._table_name + ".json"
        fn = self._table_name + ".json"
        with open(fn, "r") as infile:
            d = json.load(infile)

            state = d['state']
            self._table_name = state['table_name']
            self._column_names = state['column_names']
            self._primary_key_columns = state['primary_key_columns']
            self._next_row_id = state['next_rid']
            self._rows=d['rows']

            for k,v in d['indexes'].items():
                idx = Index(loadit=v,table=self)
                if self._indexes is None:
                    self._indexes = {}
                self._indexes[k] = idx
    def load_from_rows(self, table_name, rows):
        self._table_name = table_name
        self._rows = {}
        self._column_names = None
        self._indexes = None
        self._next_row_id = 1
        for k, r in rows.items():
            if self._column_names is None:
                self._column_names = list(sorted(r.keys()))
            self._add_row(r)
    def import_data(self, rows):
        for r in rows:
            self._add_row(r)

    def add_index(self, index_name, column_list, kind):
        if index_name is None or column_list is None or kind is None:
            raise ValueError("Not enough info")

        if self._indexes is None:
            self._indexes = {}

        #check for duplicate which prof is too lazy for?
        if index_name in self._indexes.keys():
            raise ValueError("Duplicate Index")

        idx = self._indexes.get(index_name,None)
        if idx is not None:
            raise ValueError("Duplicate Index name")

        idx = self._indexes.get("PRIMARY",None)
        if idx is not None and kind == "PRIMARY":
            raise ValueError("Duplicate primary key")

        cs = set(self._column_names)
        ks = set(column_list)
        if not ks.issubset(cs):
            raise ValueError("index columns is not subset of table columns")

        self._indexes[index_name] = Index(name=index_name,table=self,columns=column_list,kind=kind)
        self.build(index_name)
    def drop_index(self, index_name):
        del self._indexes[index_name]

    def get_specific_where(self, wc):
        if wc is None:
            return None
        result = {}
        for k,v in wc.items():
            kk = k.split(".")
            if len(kk) == 1:
                result[k] = v
            elif kk[0] == self._table_name:
                result[kk[1]] = v
        if result == {}:
            result = None
        return result
    def get_specific_project(self, p_clause):
        result = []
        if p_clause is not None:
            for k in p_clause:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])
        if result == []:
            result = None
        return result
    @staticmethod
    def on_clause_to_where(on_c, r):
        result = {c:r[c] for c in on_c}
        return result
    def get_index_and_selectivity(self, cols):
        on_template = dict(zip(cols, [None]*len(cols)))
        best = None
        n = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows.keys()))/(self._indexes[n].get_no_of_entries())

        return n, best
    @staticmethod
    def _get_scan_probe(l_table, r_table, on_clause):
        s_best, s_selective = l_table.get_index_and_selectivity(on_clause)
        r_best, r_selective = r_table.get_index_and_selectivity(on_clause)

        result = l_table, r_table

        if s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is None and r_best is not None:
            result = r_table, l_table
        elif s_best is not None and r_best is None:
            result = l_table, r_table
        elif s_best is not None and r_best is not None and s_selective < r_selective:
            result = r_table, l_table
        return result

    def join(self, r_table, on_clause, w_clause, p_clause, optimize=True):
        s_table, p_table = self._get_scan_probe(self,r_table,on_clause)

        if s_table != self and optimize:
            logging.debug("Swapping tables.")
        else:
            logging.debug("Not swapping tables.")
        logging.debug("Before pushdown, scan rows = %s", len(s_table.get_rows()))

        if optimize:
            s_tmp = s_table.get_specific_where(w_clause)
            s_proj = s_table.get_specific_project(p_clause)

            s_rows = s_table.find_by_template(s_tmp, s_proj)
            logging.debug("After pushdown, scan rows = %s", len(s_rows.get_rows()))
        else:
            s_proj = s_table.get_specific_project(p_clause)
            s_rows = s_table._project_dict(s_table.get_rows(), s_proj)
            t = CSVDataTable(s_table._table_name, loadit = True)
            t.load_from_rows(s_table._table_name, s_rows)
            s_rows = t

        scan_rows = s_rows.get_rows()

        result = []

        for k,r in scan_rows.items():
            p_where = CSVDataTable.on_clause_to_where(on_clause, r)
            p_project = p_table.get_specific_project(p_clause)

            p_rows = p_table.find_by_template(p_where, p_project).get_rows()

            if p_rows:
                for k,r2 in p_rows.items():
                    new_r = {**r, **r2}
                    result.append(new_r)

            # change result(rows) from list to dict
            result_ = {}
            for i in range(len(result)):
                result_[i] = result[i]

            tn = "JOIN("+self.get_table_name()+","+r_table.get_table_name()+")"
            final_result = CSVDataTable(table_name=tn, loadit=True)
            final_result.load_from_rows(table_name=tn, rows= result_)

            return final_result

    #missing
    def __get_sub_where_template(self, where_template):
        pass
    def __get_on_template__(self, row, on_fields):
        pass
    def __join_rows__(self, row, rows, on_fields):
        pass
    def __table_from_rows__(self, name, something, rows):
        pass
    def __get_access_path__(self, on_fields):
        on_template = dict(zip(on_fields, [None] * len(on_fields)))
        best = None
        n = self.get_best_index(on_template)
        if n is not None:
            best = len(list(self._rows.keys())) / (self._indexes[n].get_no_of_entries())
        return n, best
    def __choose_scan_probe_table__(self,right_r,on_fields):
        left_path, left_count  = self.__get_access_path__(on_fields)
        right_path, right_count = right_r.__get_access_path__(on_fields)
        if left_path is None and right_path is None:
            return self, right_r
        elif left_path is None and right_path is not None:
            return self, right_r
        elif left_path is not None and right_path is None:
            return right_r, self
        elif right_count < left_count:
            return self, right_r
        else:
            return right_r, self
    def join_old(self, right_r, on_fields, where_template = None, project_fields = None, optimize = True):
        scan_t, probe_t = self.__choose_scan_probe_table__(right_r, on_fields)
        if right_r != probe_t and optimize:
            logging.debug("Swapping tables")
        else:
            logging.debug("Not swapping tables")
        logging.debug("Before push down, scan rows = %s ", len(scan_t.get_rows()))

        probe_sub_template = None
        if optimize:
            scan_sub_template = scan_t.__get_sub_where_template(where_template)
            probe_sub_template = probe_t.__get_sub_where_template(where_template)
            scan_rows = self.find_by_template(tmp=scan_sub_template, fields=project_fields).get_rows()
            logging.debug("After push down, scan rows = %s ",len(scan_rows.get_rows()))
        else:
            scan_rows = scan_t.get_rows()

        join_result = []
        for k,l_r in scan_rows:
            on_template = self.__get_on_template__(l_r, on_fields)
            if probe_sub_template is not None:
                right_where = {**on_template, **probe_sub_template}
            else:
                right_where = on_template

            current_right_rows = right_r.find_by_template(right_where)
            if current_right_rows is not None and len(current_right_rows)>0:
                new_rows = self.__join_rows__([l_r], current_right_rows, on_fields)
                join_result.extend(new_rows)

        join_result_ = self.__table_from_rows__(
            "JOIN("+self._table_name+","+right_r._table_name+")", None, join_result)

        return join_result_


