import numpy as np
from collections import OrderedDict


class ProcessingStep:

    def __init__(self, name, candidate_properties, fct_str, import_module, constraints=()):
        self.name = name
        self._config = {"analysis": {}}

        if candidate_properties is not None:
            self.candidate_properties = sorted(candidate_properties)
        else:
            self.candidate_properties = None

        self.fct_str = fct_str
        self.import_module = import_module
        self.constraints = constraints
        self.dep_after = []

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        if config is None:
            return

        self._config = config
        if "analysis" not in self._config:
            self.config["analysis"] = {}
        if self._config["analysis"] is None:
            self.config["analysis"] = {}

        if self.candidate_properties is None:
            dep_after_properties = []
            if self.dep_after is not None:
                for dependency in self.dep_after:
                    dep_after_properties.extend(list(list(dependency.keys())[0].keys()))
                dep_after_properties = np.unique(dep_after_properties)

            self.candidate_properties = np.unique(np.concatenate((dep_after_properties,
                                                                  self._config["analysis"])))

    #@property
    #def config_path(self):
    #    if self._config is None:
    #        return None
    #    return self._config.path

    def get_dependency_ids(self, job_key):
        dep_job_ids = []
        if self.dep_after is not None:
            for dependency in self.dep_after:
                dep_job_ids.extend([dep_job_id for key_dep, dep_job_id in dependency.items()
                                    if key_dep in job_key])
        return dep_job_ids

    def get_loop_properties(self):

        properties = OrderedDict()

        dep_after_key0 = []
        if self.dep_after is not None:
            for dependency in self.dep_after:
                if len(dependency):
                    dep_after_key0.extend(list(dependency.keys())[0])
            dep_after_key0 = np.unique(dep_after_key0).tolist()

        for property_name in self.candidate_properties:

            # If candidate properties present in the dep_after, take its values from there
            if property_name in dep_after_key0:
                property_values = []
                for dependency in self.dep_after:
                    for dep_after_key in dependency.keys():
                        property_values.append(dep_after_key[property_name])
                    if property_name in properties:
                        assert (np.all(properties[property_name] == np.unique(property_values)))
                    else:
                        properties[property_name] = np.unique(property_values)

            else:
                if property_name in self.config["analysis"]:
                    # if len(self.config["analysis"][property_name]) > 1:
                    properties[property_name] = self.config["analysis"][property_name]

        for property_name in properties:
            if property_name in self.constraints:
                if isinstance(self.constraints[property_name], str):
                    self.constraints[property_name] = [self.constraints[property_name]]
                filtered_property_values = [p for p in properties[property_name]
                                            if p in self.constraints[property_name]]
                properties[property_name] = filtered_property_values

        return properties
