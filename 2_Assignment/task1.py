import os
from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime as dt
from haversine import haversine, Unit


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

                   FOREIGN KEY (user_id) 
                        REFERENCES User(id) 
                        ON DELETE CASCADE
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
                   altitude INT,
                   data DATETIME,

                   FOREIGN KEY (activity_id) 
                        REFERENCES Activity(id)
                        ON DELETE CASCADE
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_users(self, table_name):
        for u in self.users:
            if u == ".DS_Store":
                continue
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


        for user in users:
            user_id, has_labels = user
            if user_id == ".DS_Store":
                continue
            print("UserID", user_id)
            activities = os.listdir("dataset/dataset/Data/" + user_id + "/Trajectory")

            for activity in activities:
                path = "dataset/dataset/Data/" + user_id + "/Trajectory/" + activity

                if os.stat(path).st_size > 200000:
                    continue

                file = (
                    open(path)
                    .read()
                    .split("\n")
                )
                if len(file) > 2506 or file == ".DS_Store":
                    continue

                list_of_tp_string = file[6:]
                trackpoints = []
                for trackpoint in list_of_tp_string:
                    t_split = trackpoint.split(',')
                    if len(t_split) > 2:
                        trackpoints.append(tuple(t_split))

                # Format for database
                start_date_time_raw = trackpoints[0][5] + " " + trackpoints[0][6]
                end_date_time_raw = trackpoints[-1][5] + " " + trackpoints[-1][6]

                activity_query = """INSERT INTO %s 
                            (user_id, transportation_mode, start_date_time, end_date_time) 
                            VALUES ('%s', '%s', '%s', '%s')"""

                trackpoint_query = """INSERT INTO Trackpoint 
                            (activity_id, lat, lon, altitude, data) 
                            VALUES (%s, %s, %s, %s, %s)"""

                trans_mode = "NULL"
                if has_labels:

                    # Format for conparison
                    start_date_time = dt.strptime(start_date_time_raw, "%Y-%m-%d %H:%M:%S")
                    end_date_time = dt.strptime(end_date_time_raw, "%Y-%m-%d %H:%M:%S")

                    labels_for_user = (
                        open("dataset/dataset/Data/" + user_id + "/labels.txt")
                        .read()
                        .split("\n")
                    )
                    for l in labels_for_user[1:-1]:
                        split = l.split("\t")

                        label_start = dt.strptime(split[0], "%Y/%m/%d %H:%M:%S")
                        label_end = dt.strptime(split[1], "%Y/%m/%d %H:%M:%S")

                        if (
                            start_date_time == label_start
                            and end_date_time == label_end
                        ):
                           trans_mode = split[2]

                self.cursor.execute(
                    activity_query
                    % ("Activity", user_id, trans_mode, start_date_time_raw, end_date_time_raw))

                # Gets the activity_id from the last one added to the database
                activity_id = self.cursor.lastrowid
                new_trackpoints = self.alter_trackpoint(trackpoints, activity_id)

                self.cursor.executemany(trackpoint_query, new_trackpoints)
                self.db_connection.commit()

    def alter_trackpoint(self, trackpoints, activity_id):
        new_trackpoints = []
        for point in trackpoints:
            combined_date = dt.strptime(point[5] + point[6], "%Y-%m-%d%H:%M:%S")
            point = (activity_id,) + point[:2] + point[3:4] + (combined_date,)
            new_trackpoints.append(point)
        return new_trackpoints

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

    def calculate_distance(self):
        query = """select lat, lon from Trackpoint as t join Activity as a
         on a.id=t.activity_id 
         where a.user_id='112' and YEAR(data)='2008' and a.transportation_mode='walk';"""
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        total_distance = 0
        for i in range(1,len(result)):
            total_distance += haversine(result[i-1], result[i])
        print(total_distance)

def main():
    program = None
    try:
        program = Task1()
        program.create_user_table("User")
        program.create_activity_table("Activity")
        program.create_trackpoint_table("Trackpoint")
        program.insert_users("User")
        program.insert_activity_and_trackpoints()
        program.calculate_distance()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
