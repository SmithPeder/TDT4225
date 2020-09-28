import os
from DbConnector import DbConnector
from tabulate import tabulate


class Task1:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.labels = open("dataset/dataset/labeled_ids.txt", "r").read().split("\n")
        self.users = os.listdir("dataset/dataset/Data")

    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(10) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(10) NOT NULL,
                   transportation_mode VARCHAR(30),
                   start_date_time DATETIME,
                   end_date_time DATETIME,

                   FOREIGN KEY (user_id) REFERENCES User(id)
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT NOT NULL,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitiude INT,
                   date_days DOUBLE,
                   date_time DATETIME,

                   FOREIGN KEY (activity_id) REFERENCES Activity(id)
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_users(self, table_name):
        for u in self.users:
            if u in self.labels:
                query = "INSERT IGNORE INTO %s (id,has_labels) VALUES ('%s',True)"
                self.cursor.execute(query % (table_name, u))
                continue
            query = "INSERT IGNORE INTO %s (id,has_labels) VALUES ('%s',False)"
            self.cursor.execute(query % (table_name, u))
        self.db_connection.commit()

    def insert_activity_and_trackpoints(self):
        # Henter brukere fra Database
        table_name = "User"
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        users = self.cursor.fetchall()

        for user in users[1:]:
            user_id, has_labels = user
            print(user_id)
            activities = os.listdir("dataset/dataset/Data/" + user_id + "/Trajectory")

            for activity in activities:
                file = (
                    open("dataset/dataset/Data/" + user_id + "/Trajectory/" + activity)
                    .read()
                    .split("\n")
                )
                if len(file) > 2500:
                    continue

                list_of_tp_string = file[6:]
                trackpoints = []
                for c in list_of_tp_string:
                    if len(c.split(",")) > 2:
                        trackpoints.append(tuple(c.split(",")))

                # Format for database
                start_date_time_raw = trackpoints[0][5] + trackpoints[0][6]
                end_date_time_raw = trackpoints[-1][5] + trackpoints[-1][6]

                if has_labels:
                    # Format for assertion
                    start_date_time = "".join(
                        e for e in start_date_time_raw if e.isalnum()
                    )
                    end_date_time = "".join(e for e in end_date_time_raw if e.isalnum())

                    # print("START", start_date_time)
                    # print("END", end_date_time)

                    labels_for_user = (
                        open("dataset/dataset/Data/" + user_id + "/labels.txt")
                        .read()
                        .split("\n")
                    )
                    for l in labels_for_user[1:-1]:
                        split = l.split("\t")

                        label_start_raw = split[0]
                        label_start = "".join(e for e in label_start_raw if e.isalnum())

                        label_end_raw = split[1]
                        label_end = "".join(e for e in label_end_raw if e.isalnum())

                        # print("START", label_start)
                        # print("END", label_end)
                        if (
                            start_date_time == label_start
                            and end_date_time == label_end
                        ):
                            print("FOUND IT", split[2])

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = Task1()
        program.create_user_table("User")
        program.create_activity_table("Activity")
        program.create_trackpoint_table("Trackpoint")
        program.insert_users("User")

        program.insert_activity_and_trackpoints()

        #        program.insert_data(table_name="Person")
        #        _ = program.fetch_data(table_name="Person")
        #        program.drop_table(table_name="Person")
        #        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
