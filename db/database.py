import psycopg2
from db_helpers import read_config_ini

class Database:
    """A database client class that uses psycopg2 to connect to and interact with a PostgreSQL database."""

    def __init__(self) -> None:  
        """Initializes the database client"""     
        self.connect()

    def connect(self):
        """Connects to the PostgreSQL database."""
        settings = read_config_ini()
        self.connection = psycopg2.connect( host=settings['Database.host'], port=int(settings['Database.port']),
                                            database=settings['Database.database'], user=settings['Database.user'],
                                            password=settings['Database.password'])
        self.cursor = self.connection.cursor()

    def commit(self):
        """Commits all pending changes to the PostgreSQL database."""

        self.connection.commit()

    def execute_fetchall(self, sql_expression) -> list[tuple]:
        """Execute sql query and fetchall the results."""

        self.cursor.execute(sql_expression)
        return self.cursor.fetchall()

    def execute_commit(self, sql_expression):
        """Execute change to db and commit it."""

        self.cursor.execute(sql_expression)
        self.connection.commit()
    
    def executemany_commit(self, sql_expression, input):
       """Execute many inputs to db"""

       self.cursor.executemany(sql_expression, input)
       self.connection.commit()

    def disconnect(self):
        """Disconnects from the PostgreSQL database."""

        self.cursor.close()
        self.connection.close()