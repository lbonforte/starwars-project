import requests
import pymongo


# class to pull info from the api
class Extract:
    # initialisation
    def __init__(self):
        self.starships_url = "https://swapi.dev/api/starships"
        self.db = pymongo.MongoClient()['starwars']

        self.starships_data = []
        self.starship_names_list = []
        self.pilot_urls_nested_list = []
        self.pilots_list = []

        self.retrieve_starship_info()

    # method that pulls the starship data
    def retrieve_starship_info(self):
        r = requests.get(self.starships_url)
        if r.status_code == 200:
            request = r.json()
            self.starships_data += request["results"]
            while request["next"]:
                r = requests.get(request["next"])
                if r.status_code == 200:
                    request = r.json()
                    self.starships_data += request["results"]
        self.retrieve_starships()
        self.retrieve_pilot_urls()

    # method that retrieves starship names w/ pilots
    def retrieve_starships(self):
        for i in self.starships_data:
            for key, value in i.items():
                if key == "pilots" and len(value) != 0:
                    self.starship_names_list.append(i["name"])

    # create nested list of pilots urls
    def retrieve_pilot_urls(self):
        for i in self.starships_data:
            for key, value in i.items():
                if key == "pilots" and len(value) != 0:
                    self.pilot_urls_nested_list.append(value)
        self.retrieve_pilot_names()

    # create nested list of pilot names
    def retrieve_pilot_names(self):
        self.pilot_names_nested_list = self.pilot_urls_nested_list.copy()
        for idx1, (item) in enumerate(self.pilot_names_nested_list):
            for idx2, url in enumerate(item):
                pilot_r = requests.get(url)
                if pilot_r.status_code == 200:
                    pilot_data = pilot_r.json()
                    self.pilot_names_nested_list[idx1][idx2] = pilot_data["name"]
        self.retrieve_pilot_object_ids()

    # create 2 lists with pilot ids and pilot names from mongodb database
    def retrieve_pilot_object_ids(self):
        for pilots in self.pilot_names_nested_list:
            for pilot in pilots:
                self.pilots_list.append(pilot)

        pilot_ids = list(self.db.characters.find({"name": {"$in": self.pilots_list}}, {"_id": 1, "name": 1}))

        return [items['_id'] for items in pilot_ids], [items["name"] for items in pilot_ids]

