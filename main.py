import sqlite3
import random
import os


class Database:
    def __init__(self, main_path: str, database: str) -> None:
        """
        SQL database object
        :param main_path: folder where the database is located
        :param database: database file name
        """
        if os.path.exists(main_path):
            self.main_path = main_path
        else:
            raise FileExistsError
        self.database = database

    def tables(self) -> list:
        """
        Function check existing tables in db
        :return: list of tables
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()

        table_list = []
        for i in cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall():
            # cur.execute(f"""DROP TABLE IF EXISTS {i[0]}""")
            table_list.append(i[0])
        return table_list

    def create(self, table_name: str, columns: list) -> bool:
        """
        Function create new table in database
        :param table_name: name of the table
        :param columns: list of table colum
        :return: whether the table created
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()

        # check if table already exist
        res = [i[0] for i in cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()]
        if table_name in res:
            return False
        else:
            cur.execute(f"""CREATE TABLE {table_name} ({', '.join([str(i) for i in columns])})""")
            return True

    def drop(self, table_name: str) -> bool:
        """
        Function drop the table
        :param table_name: name the table to be dropped
        :return: whether the table dropped
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()

        start_res = cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()

        # check if table already exist
        if table_name == 'all' or table_name == '*':
            for i in cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall():
                cur.execute(f"""DROP TABLE IF EXISTS {i[0]}""")
        else:
            res = [i[0] for i in cur.execute(f"""DROP TABLE IF EXISTS {table_name}""").fetchall()]

        end_res = cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()

        result = (start_res == end_res)
        return result

    def get(self, table_name: str, columns='*') -> list:
        """
        Function get information from the table
        :param table_name: name of the table from witch the information will be taken
        :param columns: columns from the table
        :return: list of information
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()
        res = [i[0] for i in cur.execute(f"""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()]
        if table_name in res:
            if columns == '*':
                result = [i for i in cur.execute(f"""SELECT {columns} FROM {table_name}""").fetchall()]
                return result

            result = [i for i in
                        cur.execute(f"""SELECT {", ".join([str(i) for i in columns])} FROM {table_name}""").fetchall()]
            return result
        else:
            return []

    def insert(self, table: str, insert_info: list) -> bool:
        """
        Function to insert information into the table
        :param table: name of the table in witch the information would insert
        :param insert_info: list whit information which would insert
        :return: whether information insert
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()

        start_res = (cur.execute(f"""SELECT * FROM {table}""").fetchall())

        cur.execute(f"""INSERT INTO {table} VALUES ({('?, '*len(insert_info))[:-2]})""", insert_info)

        end_res = (cur.execute(f"""SELECT * FROM {table}""").fetchall())

        con.commit()
        con.close()

        result = (start_res == end_res)
        return result

    def delete(self, table: str, column_name: str, arg: str) -> bool:
        """
        Function to delete line from table
        :param table: name of the table
        :param column_name: name of the column with argument
        :param arg: argument
        :return: whether line deleted
        """
        con = sqlite3.connect(os.path.join(self.main_path, f"{self.database}"))
        cur = con.cursor()

        start_res = (cur.execute(f"""SELECT * FROM {table}""").fetchall())

        cur.execute(f"""DELETE FROM {table} WHERE {column_name} = '{arg}'""")

        end_res = (cur.execute(f"""SELECT * FROM {table}""").fetchall())

        con.commit()
        con.close()

        result = (start_res == end_res)
        return result
