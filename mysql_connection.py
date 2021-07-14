from getpass import getpass
from mysql.connector import connect, Error


def connect_to_db():
    """
    Conencts to specified mysql database
    @return: connection object
    """
    try:
        with connect(
            host='localhost',
            user=input('Enter username: '),
            password=getpass('Enter password: '),
            database=input('Existing Database name: ')
        ) as connection:
            print(connection)
            return connection
    except Error as e:
        print(e)


if __name__ == '__main__':
    connect_to_db()
