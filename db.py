import sqlite3
import threading
from sqlite3 import Error
import json


def format_value(value, field_type):
    if field_type == 'TEXT':
        return "'" + value + "'"
    else:
        return value


class Db:
    db_path = "./"
    conn = None
    db = None
    schema = None
    logger = None
    custom_field_prefix = 'custom_'
    lock = threading.Lock()

    def __init__(self, db_path, logger):
        self.db_path = db_path
        self.logger = logger

    def create_connection(self, schema):
        """ create a database connection to a SQLite database """
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.isolation_level = None
            # self.conn.row_factory = sqlite3.Row The algorithm below is better at transforming each row into a dict
            self.conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            self.db = self.conn.cursor()
            self.schema = schema
        except Error as e:
            self.logger.error('Error creating local database connection: %s', e)

    def create_tables(self):
        try:
            for table_name in self.schema:
                self.create_table(table_name)

        except Error as e:
            self.logger.error('Error creating database tables: %s', e)

    def delete_tables(self):
        try:
            for table_name in self.schema:
                sql = """ DROP TABLE IF EXISTS %s """ % table_name
                self.db.execute(sql)
        except Error as e:
            self.logger.error('Error deleting database tables: %s', e)

    def insert_records(self, table_name, records):
        self.db.execute("begin")
        for record in records:
            values = ()
            sql = "INSERT INTO " + table_name + "("
            for field in self.schema[table_name]["fields"]:
                if field == 'VersionData' or field == 'Body' or field == 'newId':
                    continue
                    # record[field] = None
                values += (record[field],)
                sql += field + ","
            sql = sql[:-1]
            sql += ")"
            sql += " values ("
            for field in self.schema[table_name]["fields"]:
                if field == 'VersionData' or field == 'Body' or field == 'newId':
                    continue
                sql += "?,"
            sql = sql[:-1]
            sql += ")"

            sql += " ON CONFLICT (Id) DO UPDATE SET "
            for field in self.schema[table_name]["fields"]:
                if field == 'VersionData' or field == 'Body' or field == 'newId':
                    continue
                sql += field + "=?,"
            sql = sql[:-1]
            sql += ";"
            # print(sql)
            values += values
            # print(values)
            try:
                self.db.execute(sql, values)
            except Error as e:
                self.logger.error('Error inserting database records in table %s: %s', table_name, e)
        try:
            self.db.execute("commit")
        except Error as e:
            self.logger.error('Error inserting database records in table %s: %s', table_name, e)
            self.db.execute("rollback")



    def update_records(self, table_name, fields_to_update, records, key='Id'):
        self.db.execute("begin")

        for record in records:
            print(record['VersionData'])
            fields_str = ''
            for field in fields_to_update:
                fields_str += field + " = ?,"
            fields_str = fields_str[:-1]
            sql = 'UPDATE %s SET %s WHERE %s = ?' % (table_name, fields_str, key)

            print("UPDATE %s SET %s WHERE %s = ?" % (table_name, fields_str, key))
            print(record)
            exit()
            self.db.execute(sql, (record[1]["id"], record[key]))
        try:
            self.db.execute("commit")
        except Error as e:
            self.logger.error('Error inserting database records in table %s: %s', table_name, e)
            self.db.execute("rollback")

    def update_external_ids(self, table_name, records, external_id):
        try:
            self.lock.acquire(True)
            self.db.execute("begin")
            for record in records:
                sql = 'UPDATE %s SET newId = ? WHERE Id = ?' % table_name
                if table_name == 'articles':
                    external_id = "urlName"
                # print("UPDATE %s SET external_id = %s WHERE id = %s" % (table_name, record[1]["id"], record[0][external_id]))
                self.db.execute(sql, (record[1]["id"], record[0][external_id]))
            try:
                self.db.execute("commit")
            except Error as e:
                self.logger.error('Error inserting database records in table %s: %s', table_name, e)
                self.db.execute("rollback")
        finally:
            self.lock.release()

    def create_table(self, table_name):
        fields_sql = ""
        # add an additional field to store teh new Salesforce Id after import
        self.schema[table_name]["fields"]["newId"] = {}
        table_schema = self.schema[table_name]["fields"]
        # print(table_schema)
        for field in table_schema.keys():
            fields_sql += field + ' ' + \
                          'TEXT' + (' PRIMARY KEY' if field == 'Id' else '') + \
                          ','
        fields_sql = fields_sql[:-1]

        sql = '''CREATE TABLE IF NOT EXISTS ''' + table_name + '''('''
        sql += fields_sql
        sql += ''')'''
        # print(sql)
        try:
            self.db.execute(sql)
        except Error as e:
            self.logger.error('Error creating database table  %s: %s', table_name, e)

    def get_records(self, table_name, where_clause=None, limit=None, offset=None):
        if where_clause is not None:
            where_clause = 'WHERE %s' % where_clause
        sql = "SELECT * FROM %s %s ORDER BY id;" % (table_name, where_clause)
        if limit is not None and offset is not None:
            sql = "SELECT * FROM %s ORDER BY id LIMIT %s, %s;" % (table_name, offset, limit)
        self.db.execute(sql)
        rows = self.db.fetchall()
        return rows

    def get_record_count(self, table_name):
        sql = "SELECT count(id) FROM %s ;" % (table_name)
        res = self.db.execute(sql)
        values = res.fetchone()
        print('Found %s %s' % (values['count(id)'], table_name))
        return values['count(id)']
