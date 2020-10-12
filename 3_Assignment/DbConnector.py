from pymongo import MongoClient, version
import os


class DbConnector:
    
    def __init__(self):
        HOST = os.environ.get('HOST')
        DATABASE = os.environ.get('DATABASE')
        USER = os.getenv('USER')
        PASSWORD = os.getenv('PASSWORD')

        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the databases

        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
