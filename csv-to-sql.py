import string
import pandas as pd
from config import HOST, DBNAME, USER, PASS
import sqlalchemy as db

df = pd.read_csv('Policydata_2022-04-05.csv',
                 delimiter=";", dtype='unicode')
df = df.fillna('0')


columns = [x.lower().translate(str.maketrans('', '', string.punctuation)).replace(
    " ", "_") for x in df.columns]

values = df.values.tolist()


tbl_name = 'policydata'

conn_dict = {
    "host": HOST,
    "db_name": DBNAME,
    "user": USER,
    "password": PASS
}


##############################
# OPEN DB
##############################
try:
    db_connection_str = 'mysql+pymysql://{user}:{password}@{host}/{db_name}'.format(
        **conn_dict)
    db_connection = db.create_engine(db_connection_str)
    connection = db_connection.engine.raw_connection()
    cursor = connection.cursor()
    print('>>>> OPENNED DB <<<<')
except db.exc.SQLAlchemyError as e:
    print(e)


##############################
# DROP TABLE
##############################
try:
    db_connection.execute("drop table if exists {};".format(tbl_name))
    cursor.commit()
    print('>>>> TABLE DROPPED <<<<')
except db.exc.SQLAlchemyError as e:
    print(e)
    print('CAN`T DROP TABLE')


# TODO CHECK TYPES
replacements = {
    'timedelta64[ns]': 'text',
    'object': 'text',
    'float64': 'float',
    'int64': 'int',
    'datetime64': 'timestamp'
}

cols_with_typings = ", ".join("{} {}".format(n, d) for (n, d) in zip(
    columns, df.dtypes.replace(replacements)))

##############################
# CREATE TABLE WITH COLUMNS
##############################
try:
    query = 'CREATE TABLE {} ({});'.format(
        tbl_name, cols_with_typings)
    db_connection.execute(query)
    print('>>>> TABLE CREATED <<<<')
except db.exc.SQLAlchemyError as e:
    print(e)
    print('CAN`T CREATE TABLE')


##############################
# ADD DATA ROW BY ROW
##############################
try:
    col_strings = ', '.join(columns)
    for row in values:
        query = 'INSERT INTO {} ({}) VALUES {}'.format(
            tbl_name, col_strings, tuple(row))
        db_connection.execute(db.text(query))
        cursor.commit()
    print('>>>> TABLE UPDATED <<<<')
except db.exc.SQLAlchemyError as e:
    print(e)
    print('CAN`T UPDATE TABLE')

try:
    cursor.close()
    print('>>>> CONNECTION CLOSED <<<<')
except db.exc.SQLAlchemyError as e:
    print(e, 'CAN`T CLOSE?')


def get_data_from_sql():
    result = pd.read_sql(
        'SELECT * FROM {};'.format(tbl_name), con=db_connection)
    db_connection.dispose()

    return result


# print(get_data_from_sql())
