from pymongo import MongoClient


class DbConnector:
    def __init__(self,
                 DATABASE='movies_db',
                 HOST="localhost",
                 PORT=27017,
                 USER="jeswes99",
                 PASSWORD="makemeadonut"):
        if USER and PASSWORD:
            uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?authSource=movies_db"  # Adjust authSource if needed
        else:
            uri = f"mongodb://{HOST}:{PORT}/{DATABASE}"

        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
            print("You are connected to the database:", self.db.name)
        except Exception as e:
            print("Could not connect to the database:", e)
            self.client = None
            self.db = None

    def close_connection(self):
        if self.client is not None:
            self.client.close()
        if self.db is not None:
            print("\n-----------------------------------------------")
            print("Connection to %s-db is closed" % self.db.name)
