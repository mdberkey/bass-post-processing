from getpass import getpass
from mysql.connector import connect, Error
from pandas import DataFrame
import sqlalchemy as sqla


def connect_to_db():
    """
    Conencts to specified mysql database
    @return: connection object
    """
    try:
        host = 'localhost'
        user = input('Enter username: ')
        password = getpass('Enter password: ')
        engine = sqla.create_engine(f'mysql://{user}:{password}@{host}')
        print(engine)
        with engine.connect() as connection:
            connection.execute('USE bass_data;')
        return engine
    except ValueError as e:
        print(e)


def write_to_db(df, engine, tb_name=None):
    """
    Wrties dataframe to database specified in engine
    @param df: pandas dataframe of bass data
    @param engine: sqlaclchemy engine
    @param tb_name: table name if none, then name will be cummulative_data
    @return: None
    """
    if not tb_name:
        tb_name = 'cummulative_data'
    try:

        df.to_sql(tb_name, engine)
    except ValueError:
        option = input(f'Existing table named {tb_name} found.\n'
              f'To Append enter: [a]\n'
              f'To Replace enter: [r]\n'
              f'To Strop write enter: [s]\n'
              )
        if option == 'a':
            option = 'append'
        elif option == 'r':
            option = 'replace'
        else:
            return
        df.to_sql(tb_name, engine, if_exists=option)
