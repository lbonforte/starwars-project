from etl_pipeline.transform import Transform


class Load(Transform):

    def __init__(self):
        super().__init__()
        self.create_starships_collection()
        self.insert_pilots_into_starships()
        self.join_pilots_data()

    def create_starships_collection(self):
        return self.db.create_collection("starships")

    def insert_pilots_into_starships(self):
        n = 0
        while n <= (len(self.starship_names_list)-1):
            self.db.starships.insert_one({"name": list(self.starship_ids_dict.keys())[n],
                                          "pilot": list(self.starship_ids_dict.values())[n]})
            n += 1

    def join_pilots_data(self):
        return list(self.db.starships.aggregate([{"$lookup": {"from": "characters", "localField": "pilot",
                                                              "foreignField": "_id", "as": "matched_pilot"}},
                                                 {"$project": {"name": 1, "model": 1, "matched_pilot.name": 1}}]))




