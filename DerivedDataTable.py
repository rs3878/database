# Your implementation goes in this file.
from CSVDataTable import CSVDataTable

class DerivedDataTable(CSVDataTable):
    """
        The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
        base class and implement the abstract methods.
        """

    def __init__(self, table_name, rows):
        self._table_name = table_name
        super().__init__(table_name, None, None)
        self._rows = rows
        self._connect_info = None
        self._column_names = None
        self._key_columns = None

    def __str__(self):
        return super().__str__()

    def insert(self, row):
        result = [row[k] for k in self._key_columns]
        return result

    def add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def get_rows(self):
        return self._rows


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        return super().find_by_template(template,field_list,limit,offset,order_by)


    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """
        raise NotImplementedError()

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        raise NotImplementedError()

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        raise NotImplementedError()

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        raise NotImplementedError()

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        raise NotImplementedError()

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        raise NotImplementedError()
