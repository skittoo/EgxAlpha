from initializer import DBInitializer
from database import Database

database_client = Database()
init = DBInitializer(database_client)

init.initialize_db()
