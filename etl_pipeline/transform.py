from etl_pipeline.extract import Extract


# class that transform data into dictionaries
class Transform(Extract):

    def __init__(self):
        super().__init__()
        self.id_strings_list = []
        self.pilot_ids_list = self.pilot_names_nested_list.copy()

        self.pilot_object_ids = self.retrieve_pilot_object_ids()
        self.pilot_id_dict = self.create_pilot_id_dict()
        self.starship_names_dict = dict(zip(self.starship_names_list, self.pilot_names_nested_list))

        self.starship_ids_dict = self.create_starship_ids_dict()

    def create_pilot_id_dict(self):
        # convert ids into string
        for i in self.pilot_object_ids[0]:
            self.id_strings_list.append(f'ObjectId("{str(i)}")')
        # create a zip object and convert it to dictionary
        return dict(zip((self.pilot_object_ids[1]), self.pilot_object_ids[0]))

    def create_starship_ids_dict(self):
        # change pilot names list to ids
        for idx1, names in enumerate(self.pilot_ids_list):
            for idx2, name in enumerate(names):
                self.pilot_ids_list[idx1][idx2] = self.pilot_id_dict[name]

        # create a zip object and convert it to dictionary
        return dict(zip(self.starship_names_list, self.pilot_ids_list))

