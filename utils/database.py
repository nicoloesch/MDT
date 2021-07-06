import sqlite3
import os
from typing import Union, List
import pickle
import datetime

# NOTE: it is not best practice with the with statements and directly use a connection but it is also not forbidden and
# makes the code nice and clean as the with statement terminates the connection to the database after execution

CREATE_BLOOD_TABLE = """
    CREATE TABLE IF NOT EXISTS blood (
    id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,
    cholesterol REAL,
    triglyceride REAL,
    protein REAL,
    leuko REAL,
    erythro REAL,
    haemo REAL,
    gluc REAL,
    uric_acid REAL,
    bili REAL,
    phosphate REAL,
    gamma_gt REAL,
    got REAL,
    gpt REAL,
    ldh REAL,
    creatinine REAL,
    urea REAL,
    sodium REAL,
    potassium REAL,
    calcium REAL,
    iron REAL,
    hematocrite REAL,
    mcv REAL,
    mch REAL,
    mchc REAL,
    thrombo REAL,
    lipase REAL,
    tsh REAL,
    transferrin REAL,
    ebv_igg REAL,
    ebv_igm REAL,
    tpo_ab REAL,
    t3 REAL,
    t4 REAL,
    b12 REAL,
    crp REAL,
    lyme_igg REAL,
    lyme_igm REAL,
    anti_ebv REAL,
    vit_d REAL,
    UNIQUE (date));"""

INSERT_BLOOD = """INSERT INTO blood (date, cholesterol, triglyceride, protein, leuko, erythro, haemo, gluc, uric_acid, 
bili, phosphate, gamma_gt, got, gpt, ldh, creatinine, urea, sodium, potassium, calcium, iron, hematocrite, mcv,
mch, mchc, thrombo, lipase, tsh, transferrin, ebv_igg, ebv_igm, tpo_ab, t3, t4, b12, crp, lyme_igg, lyme_igm, anti_ebv, 
vit_d) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""


class SQLiteDatabase:
    def __init__(self, database_path: str, database_name: str):
        """Connect to database as initialization

            :param database_path: path to the database
            :param database_name: name of the database
            """
        self.connection = sqlite3.connect(os.path.join(database_path, database_name))
        self.database = database_name

    def create_blood_table(self):
        """ Create a table within a connected database with columns\n
            id, origin, destination, name
            """
        with self.connection:
            self.connection.execute(CREATE_BLOOD_TABLE)

    def add_sample(self,
                   year: int,
                   month: int,
                   day: int,
                   cholesterol: float = None,
                   triglyceride: float = None,
                   protein: float = None,
                   leuko: float = None,
                   erythro: float = None,
                   haemo: float = None,
                   gluc: float = None,
                   uric_acid: float = None,
                   bili: float = None,
                   phosphate: float = None,
                   gamma_gt: float = None,
                   got: float = None,
                   gpt: float = None,
                   ldh: float = None,
                   creatinine: float = None,
                   urea: float = None,
                   sodium: float = None,
                   potassium: float = None,
                   calcium: float = None,
                   iron: float = None,
                   hematocrite: float = None,
                   mcv: float = None,
                   mch: float = None,
                   mchc: float = None,
                   thrombo: float = None,
                   lipase: float = None,
                   tsh: float = None,
                   transferrin: float = None,
                   ebv_igg: float = None,
                   ebv_igm: float = None,
                   tpo_ab: float = None,
                   t3: float = None,
                   t4: float = None,
                   b12: float = None,
                   crp: float = None,
                   lyme_igg: float = None,
                   lyme_igm: float = None,
                   anti_ebv: float = None,
                   vit_d: float = None) -> bool:
        """ Add a blood sample to the database blood.

            :param int year: year of the sample
            :param int month: month of the sample
            :param int day: day of the sample
            :param  float cholesterol: [mg/dl]
            :param  float triglyceride: [mg/dl]
            :param  float protein: [mg/dl]
            :param  float leuko: [mg/nl]
            :param  float erythro: [/pl]
            :param  float haemo: [g/dl]
            :param  float gluc: [mg/dl]
            :param  float uric_acid: [mg/dl]
            :param  float bili: [mg/dl]
            :param  float phosphate: [U/l]
            :param  float gamma_gt: [U/l]
            :param  float got: [U/l]
            :param  float gpt: [U/l]
            :param  float ldh: [U/l]
            :param  float creatinine: [mg/dl]
            :param  float urea: [mg/dl]
            :param  float sodium: [mmol/l]
            :param  float potassium: [mmol/l]
            :param  float calcium: [mmol/l]
            :param  float iron: [mg/dl]
            :param  float hematocrite: [vol%]
            :param  float mcv: [ftl]
            :param  float mch: [pg]
            :param  float mchc: [g/l]
            :param  float thrombo: [/nl]
            :param  float lipase: [U/l]
            :param  float tsh: [mU/l]
            :param  float transferrin: [mg/dl]
            :param  float ebv_igg: [U/ml]
            :param  float ebv_igm: [U/ml]
            :param  float tpo_ab: [U/ml]
            :param  float t3: [ng/l]
            :param  float t4: [ng/dl]
            :param  float b12: [pg/ml]
            :param  float crp: [mg/dl]
            :param  float lyme_igg: [AU/ml]
            :param  float lyme_igm: [AU/ml]
            :param  float anti_ebv: [U/ml]
            :param  float vit_d: [µg/l]
        """
        date = datetime.date(year, month, day)
        try:
            with self.connection:
                self.connection.execute(INSERT_BLOOD, (str(date), cholesterol, triglyceride, protein, leuko, erythro,
                                                       haemo, gluc, uric_acid, bili, phosphate, gamma_gt, got, gpt, ldh,
                                                       creatinine, urea, sodium, potassium, calcium, iron, hematocrite,
                                                       mcv, mch, mchc, thrombo, lipase, tsh, transferrin, ebv_igg,
                                                       ebv_igm, tpo_ab, t3, t4, b12, crp, lyme_igg, lyme_igm, anti_ebv,
                                                       vit_d))
            return True

        # could be prevented by making the statement INSERT_VIDEO to INSERT OR REPLACE
        # but this increases the unique file id in the beginning and i dont want that
        except sqlite3.IntegrityError:
            print(f"Duplicate sample with same date ({date}) found. Skipping file")
            return False

    def get_table_names(self):
        """ Get all tables within one database

            :returns List: List of all tables within the database
        """
        try:
            with self.connection:
                return self.connection.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

        # could be prevented by making the statement INSERT_VIDEO to INSERT OR REPLACE
        # but this increases the unique file id in the beginning and i dont want that
        except sqlite3.IntegrityError:
            print("Error")
            return False

    def get_entries_all(self, table_name: str) -> List[list]:
        """ Get all the entries within the specified table

            :param str table_name: name of the table from which to get entries
            :returns: List of tuples with all columns and rows of the table
            """
        try:
            with self.connection:
                ret = self.connection.execute(f"SELECT * FROM {table_name};").fetchall()
                return check_for_bytes(ret)
        except sqlite3.OperationalError:
            print(f"Accessing wrong table {table_name}."
                  f"Available tables are {[tab[0] for tab in self.get_table_names()]}")

    def get_entries_specific(self, table_name: str, column_name: str, entry_name: str):
        """ Get all the entries where the entry_name is in the column specified by column_name

            :param str entry_name: exact (?) string of the elements which are searched for. I.e "video" would give all
            rows with "video" as entry of the specified column
            :param str table_name: name of the table from which to get entries
            :param str column_name: name of the column in which to search for the entry_name
            :returns: List of tuples with all columns and rows of the table
            """
        if table_name == "labels" and column_name == "label_list":
            raise AttributeError("Entries are stored as bytes and can't be searched")
        try:
            with self.connection:
                ret = self.connection.execute(f"SELECT * FROM {table_name} WHERE {column_name} = ?;",
                                              (entry_name,)).fetchall()
                return check_for_bytes(ret)
        except sqlite3.OperationalError as err:
            print(err)

    def get_entries_of_column(self, table_name: str, column_name: str):
        """ Get all the entries within the table by the specifier of the column

            :param str table_name: name of the table from which to get entries
            :param str column_name: name of the column in which to search for the entry_name
            :returns: List of tuples with all columns and rows of the table
            """
        try:
            with self.connection:
                ret = self.connection.execute(f"SELECT {column_name} FROM {table_name};").fetchall()
                return check_for_bytes(ret)
        except sqlite3.OperationalError:
            print(f"Accessing wrong table {table_name}."
                  f"Available tables are {[tab[0] for tab in self.get_table_names()]}")

    def get_num_entries(self, table_name: str):
        """ Get the total number of entries

            :param str table_name: name of the table from which to get entries
            :returns: Number of total entries
            """
        try:
            with self.connection:
                return self.connection.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        except sqlite3.OperationalError:
            print(f"Accessing wrong table {table_name}."
                  f"Available tables are {[tab[0] for tab in self.get_table_names()]}")

    def get_num_entries_specific(self, table_name: str, column_name: str, entry_name: str) -> int:
        """ Returns number of entries which match the specifier entry_name of the column

            :param str entry_name: name of the video one wants
            :param str table_name: name of the table from which to get entries
            :param str column_name: name of the column in which to search for the entry_name
            :returns: List of tuples with all columns and rows of the table
            """
        if table_name == "labels" and column_name == "label_list":
            raise AttributeError("Entries are stored as bytes and can't be searched")
        try:
            with self.connection:
                return self.connection.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = ?;",
                                               (entry_name,)).fetchone()[0]
        except sqlite3.OperationalError:
            print(f"Accessing wrong table {table_name}."
                  f"Available tables are {[tab[0] for tab in self.get_table_names()]}")

    def delete_table(self, table_name: str):
        """ Delete entire table

            :param str table_name: name of the table where to delete
            """
        try:
            with self.connection:
                return self.connection.execute(f"DROP TABLE IF EXISTS {table_name};")
        except sqlite3.OperationalError:
            print(f"Accessing wrong table {table_name}."
                  f"Available tables are {[tab[0] for tab in self.get_table_names()]}")

    def change_specific_entry(self, table_name: str, row_id: int, column_name: str, new_value: str):
        """ Change one specific entry based on the row_id, column_name and table_name,

            :param str table_name: name of the table where to alter the entry
            :param int row_id: ID of the row
            :param str column_name: name of the column the entry is in
            :param str new_value: new value to replace the old with
            """
        try:
            with self.connection:
                # NOTE: pure f string formatting didnt work so its a mixture. Don't know why
                self.connection.execute(f"""UPDATE {table_name} SET {column_name} = ? WHERE id = {row_id} ;""",
                                        (new_value,))
        except sqlite3.OperationalError as err:
            print(err)

    def replace_specific(self, table_name: str, column_name, keyword: str, replacement: str):
        """ Change all entries within a table and column that contain a certain string

            :param str table_name: name of the table where to alter the entry
            :param str column_name: name of the column the entry is in
            :param str keyword: keyword to search for
            :param str replacement: replacement value
            """
        if table_name == "labels" and column_name == "label_list":
            raise AttributeError("Entries are stored as bytes and can't be accessed")
        try:
            with self.connection:
                # NOTE: pure f string formatting didnt work so its a mixture. Don't know why
                self.connection.execute(
                    f"""UPDATE {table_name} SET {column_name} = 
                    REPLACE({column_name}, ?, ?);""",
                    (keyword, replacement,))

        except sqlite3.OperationalError as err:
            print(err)

    def rename_column(self, table_name, old_column_name, new_column_name):
        """ Change all entries within a table and column that contain a certain string

            :param str table_name: name of the table where to alter the entry
            :param str old_column_name: name of the old column
            :param str new_column_name: name of the new column
            """
        try:
            with self.connection:
                # NOTE: pure f string formatting didnt work so its a mixture. Don't know why
                self.connection.execute(
                    f"""ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};""")
        except sqlite3.OperationalError as err:
            print(err)

    def rename_table(self, old: str, new: str):
        """Alters the name of an existing table

            :param str old: old table name
            :param str new: new table name
            """
        # TODO: error catching with table names if they do not exist
        with self.connection:
            self.connection.execute(f"""ALTER TABLE {old} RENAME TO {new}""")

    def update_entry(self, table_name: str, column_name_search: str,
                     keyword: str, column_name_replace: str, value_new: str):
        """ Update a single entry based on the old value in the column. Is most likely similar to
        replace_specific and change specific entry

            :param str table_name: name of the table where to alter the entry
            :param int keyword: keyword to search in column_name_search
            :param str column_name_search: name of the column the searched keyword is in
            :param str column_name_replace: column where it should be replaced
            :param str value_new: new value to replace the old with
            """
        try:
            with self.connection:
                # NOTE: pure f string formatting didnt work so its a mixture. Don't know why
                self.connection.execute(
                    f"""UPDATE {table_name} SET {column_name_replace} = ? WHERE {column_name_search} = ?;""",
                    (value_new, keyword))
        except sqlite3.OperationalError as err:
            print(err)


def check_for_bytes(lst: List[tuple]) -> Union[List[list], list]:
    """ Iterates over a list of tuples and depickles byte objects. The output is converted depending on how many entries
    the initial list contains. If its just one per sub-list, each of them is removed

        :param tuple lst: tuple to be searched for
        :returns: Either a List of Lists or just a List depending on how many entries are put in
    """
    lst = convert_to_list(lst)
    for _list_idx, _list_entry in enumerate(lst):

        # this replaces lists with only one entry
        if len(_list_entry) == 1:
            if isinstance(_list_entry[0], bytes):
                lst[_list_idx] = pickle.loads(_list_entry[0])
            else:
                lst[_list_idx] = _list_entry[0]
        else:
            for _tuple_idx, _value in enumerate(list(_list_entry)):
                if isinstance(_value, bytes):
                    lst[_list_idx][_tuple_idx] = pickle.loads(_value)
                else:
                    continue
    if len(lst) == 1:
        lst = lst[0]
    return lst


def convert_to_list(lst: List[tuple]) -> List[list]:
    return [list(elem) for elem in lst]


if __name__ == "__main__":
    database = SQLiteDatabase("/home/nico/Documents", "blood_values.db")
    database.add_sample()
