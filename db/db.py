import mysql.connector

class Database():
    def insert(self, table: str, data: dict):
        print(f"INSERT INTO {table} ({', '.join(data.keys())}) VALUES ({', '.join(data.values())})")

    def remove(self, table, condition):
        print(f"DELETE FROM {table} WHERE {condition}")

    def update(self, table, data, condition):
        print(f"UPDATE {table} SET {data} WHERE {condition}")

    def select(self, table, columns=["*"], condition=""):
        print(f"SELECT {columns} FROM {table} WHERE {condition}")

    def drop_table(self, table):
        print(f"DROP TABLE {table}")

    def add_table(self, table, columns):
        print(f"CREATE TABLE {table} ({columns})")


class MySQLDatabase:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def insert(self, table, data: dict, commit=True):
        columns = ", ".join(data.keys())
        values = ", ".join([f"%s" for _ in data.values()])
        sql_query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        try:
            self.cursor.execute(sql_query, tuple(data.values()))
            if commit:
                self.connection.commit()
        except mysql.connector.errors.Error as e:
            print()
            print(e)
            print(sql_query, tuple(data.values()))
            raise

    def remove(self, table, condition, commit=True):
        sql_query = f"DELETE FROM {table} WHERE {condition}"
        self.cursor.execute(sql_query)
        if commit:
            self.connection.commit()

    def update(self, table, data, condition, commit=True):
        set_clauses = ", ".join([f"{key}=%s" for key in data.keys()])
        sql_query = f"UPDATE {table} SET {set_clauses} WHERE {condition}"
        self.cursor.execute(sql_query, tuple(data.values()))
        if commit:
            self.connection.commit()

    def alter(self, table, statement, commit=True):
        sql_query = f"ALTER TABLE {table} {statement}"
        self.cursor.execute(sql_query)
        if commit:
            self.connection.commit()

    def select(self, table, columns=["*"], condition="", limit=None, unique=False):
        table = ', '.join(table) if isinstance(table, list) or isinstance(table, tuple) else table
        sql_query = f"SELECT {'DISTINCT ' if unique else ''}{', '.join(columns)} FROM {table}"
        if condition:
            sql_query += f" WHERE {condition}"
        if limit:
            sql_query += f" LIMIT {limit}"
        print(sql_query)
        self.cursor.execute(sql_query)
        return self.cursor.fetchall()
    
    def raw_cmd(self, cmd, results=False):
        print(f'Executing command: {cmd}')
        self.cursor.execute(cmd)
        if results:
            results = self.cursor.fetchall()
            # print(f'Results: {results}')
            return results
        self.connection.commit()
    
    def drop_table(self, table, commit=True, catch_errors=True):
        sql_query = f"DROP TABLE {table}"
        try:
            self.cursor.execute(sql_query)
            if commit:
                self.connection.commit()
        except mysql.connector.errors.ProgrammingError as e:
            if catch_errors:
                print(e)
            else:
                raise e
    
    def drop_view(self, view, commit=True, catch_errors=True):
        sql_query = f"DROP VIEW {view}"
        try:
            self.cursor.execute(sql_query)
            if commit:
                self.connection.commit()
        except mysql.connector.errors.ProgrammingError as e:
            if catch_errors:
                print(e)
            else:
                raise e
    
    def add_table(self, table: str, columns: list[str], primary_key: str | list[str] | None = None, foreign_keys: list[str] | None = None, unique_keys: list[str] | None = None, kwargs: dict[str, str] | None = None):
        if self.check_table_exists(table):
            print(f"Table {table} already exists")
            return
        sql_query = f"CREATE TABLE {table} ({', '.join(columns) })"
        try:
            self.cursor.execute(sql_query)
            self.connection.commit()
        except mysql.connector.errors.ProgrammingError as e:
            print(sql_query)
            print(e)
            raise e

    def get_columns(self, table, names_only = True) -> list[str]:
        sql_query = f"SHOW COLUMNS FROM {table}"
        self.cursor.execute(sql_query)
        if names_only:
            return [column[0] for column in self.cursor.fetchall()]
        return [column for column in self.cursor.fetchall()]

    def check_table_exists(self, table):
        self.cursor.execute(f"SHOW TABLES LIKE '{table}'")
        return self.cursor.fetchone() is not None

    def print_data(self, table):
        self.cursor.execute(f"SELECT * FROM {table}")
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)

    def print_databases(self):
        self.cursor.execute("SHOW DATABASES")
        for x in self.cursor:
            print(x)

    def get_tables(self):
        self.cursor.execute("SHOW TABLES")
        return self.cursor.fetchall()

    def print_tables(self):
        self.cursor.execute("SHOW TABLES")
        for x in self.cursor:
            print(x)


    def __del__(self):
        self.connection.close()