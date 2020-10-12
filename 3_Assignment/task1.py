from pprint import pprint
from DbConnector import DbConnector
import os
from datetime import datetime as dt


class Task1:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

        # Read all labels right away, to avoid doing it later
        self.labels = open("dataset/labeled_ids.txt", "r").read().split("\n")[:-1]

    def find_labels(self, user):
        labels_for_user = []
        if user in self.labels:
            # Read the labels for this user, skip the first line as they are headers
            labels_for_user = (
                open("dataset/Data/" + user + "/labels.txt").read().split("\n")
            )
            labels_for_user = [t.split("\t") for t in labels_for_user[1:-1]]
        return labels_for_user

    def insert(self):
        # Use os.walk to traverse
        for (root, dirs, files) in os.walk("dataset/data", topdown=True):
            # If we are not on .plt level we keep traversting
            if not any(".plt" in s for s in files):
                continue

            # If we are on .plt level, we know the username will be the [2] index of the root
            user = root.split("/")[2]

            # Get the users labels, if there are any
            labels_for_user = self.find_labels(user)

            # Hold a list of activities at this level
            activities = []

            for activity in files:
                plt_path = root + "/" + activity

                # First check if the size of the file is above 200kb, in which case it will be longer then 3000lines
                if os.stat(plt_path).st_size > 200000:
                    continue

                # Split the file into lines, each line is a trackpoint
                activity_file = open(root + "/" + activity).read().split("\n")

                # Don't continue if there are more than 2506 lines in the file
                if len(activity_file) > 2506:
                    continue

                # Only lines 6 and out are trackpoints, and we wan't to split each line on ","
                trackpoints = [t.split(",") for t in activity_file[6:-1]]

                # We can now find the start_time and end_time of the activity by using the first and last trackpoint
                start_time = dt.strptime(
                    trackpoints[0][5] + trackpoints[0][6], "%Y-%m-%d%H:%M:%S"
                )
                end_time = dt.strptime(
                    trackpoints[-1][5] + trackpoints[-1][6], "%Y-%m-%d%H:%M:%S"
                )

                trans_mode = None
                if user in self.labels:
                    for label in labels_for_user:
                        label_start = dt.strptime(label[0], "%Y/%m/%d %H:%M:%S")
                        label_end = dt.strptime(label[1], "%Y/%m/%d %H:%M:%S")

                        if (
                            start_time == label_start
                            and end_time == label_end
                        ):
                            trans_mode = label[2]

                format_activity = {
                    "userId": None,  # should be reference *pointer*. Update this in second iteration
                    "transportationMode": trans_mode,  # will be updated in next iteration
                    "startTime": start_time,
                    "endTime": end_time,
                }

                # Create activity
                activity_id = (
                    self.db["Activity"].insert_one(format_activity).inserted_id
                )
                activities.append(activity_id)

                format_trackpoints = []
                for trackpoint in trackpoints:
                    date_formatted = dt.strptime(
                        trackpoint[5] + trackpoint[6], "%Y-%m-%d%H:%M:%S"
                    )
                    format_trackpoint = {
                        "lat": trackpoint[0],
                        "lon": trackpoint[1],
                        "altitude": trackpoint[3],
                        "dateTime": date_formatted,
                        "activity": activity_id,
                    }
                    format_trackpoints.append(format_trackpoint)
                self.db["Trackpoint"].insert_many(format_trackpoints)

            # Create a user
            format_user = {
                "_id": user,
                "hasLabels": user in self.labels,
                "activities": activities,
            }
            self.db["User"].insert_one(format_user)
            print("Created user", user, "with", len(activities), "activities")

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print("Created collection: ", collection)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client["test"].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = Task1()
        program.drop_coll(collection_name="User")
        program.drop_coll(collection_name="Activity")
        program.drop_coll(collection_name="Trackpoint")

        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="Trackpoint")

        program.insert()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
