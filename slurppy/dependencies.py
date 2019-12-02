from collections.abc import Iterable
import numpy as np
from warnings import warn

from .job import KeyDict


class StepDependency:
    def __init__(self, depend_on_name: str, type_: str):
        self.depend_on_name = depend_on_name
        self.type_ = type_
        self._job_dependencies: dict = {}

    def __str__(self):
        return "StepDependency({}, {}) with {} job_dependencies".format(self.depend_on_name,
                                                                        self.type_,
                                                                        len(self._job_dependencies))

    def to_json(self):
        return {"depend_on_name": self.depend_on_name,
                "type": self.type_,
                "job_dependencies": [(key.to_json(), val) for key, val in self._job_dependencies.items()]}

    @property
    def depend_on_name(self):
        return self._depend_on_name

    @depend_on_name.setter
    def depend_on_name(self, depend_on_name: str):
        self._depend_on_name = depend_on_name

    @property
    def type_(self):
        return self._type_

    @type_.setter
    def type_(self, type_: str):
        self._type_ = type_

    def key(self):
        return "{}_{}".format(self.depend_on_name, self.type_)

    @staticmethod
    def check_job_dependency_arguments(job_key: KeyDict, slurm_ids: list):
        if not isinstance(job_key, KeyDict):
            raise TypeError("job_key must be of type KeyDict. An object of type {} was passed instead."
                            .format(type(job_key)))

        if isinstance(slurm_ids, (int, str)):
            slurm_ids = [str(slurm_ids)]
        elif isinstance(slurm_ids, Iterable):
            slurm_ids = list(slurm_ids)
        elif slurm_ids is not None:
            raise TypeError("slurm_ids should be of type list or None, but int, str, or iterables are accepted." +
                            "An incompatible type {} was passed instead."
                            .format(type(slurm_ids)))

        return job_key, slurm_ids

    def add_job_dependency(self, job_key: KeyDict, slurm_ids: list):
        job_key, slurm_ids = StepDependency.check_job_dependency_arguments(job_key, slurm_ids)

        if job_key in self._job_dependencies:
            if self._job_dependencies[job_key] is None:
                self._job_dependencies[job_key] = slurm_ids
            else:
                self._job_dependencies[job_key].extend(slurm_ids)
        else:
            self._job_dependencies[job_key] = slurm_ids

    @property
    def job_dependencies(self):
        return self._job_dependencies.copy()

    @job_dependencies.setter
    def job_dependencies(self):
        warn("Values cannot be set directly to job_dependencies. Use add_job_dependency instead.")


class Dependencies:

    def __init__(self):
        self._dep_by_depended_step = {}
        self._dep_by_job_key = {}

    @property
    def dep_by_depended_step(self):
        return self._dep_by_depended_step.copy()

    @dep_by_depended_step.setter
    def dep_by_depended_step(self):
        warn("Values cannot be set directly to dep_by_depended_step. Use add_*_dependency instead.")

    @property
    def dep_by_job_key(self):
        return self._dep_by_job_key.copy()

    @dep_by_job_key.setter
    def dep_by_job_key(self):
        warn("Values cannot be set directly to dep_by_job_key. Use add_*_dependency instead.")

    def to_json(self):
        return [dependency.to_json() for dependency in self._dep_by_depended_step.values()]

    def add_step_dependency(self, depend_on_name, type_="afterok"):
        """
         Add the dependency (depend_on_name, type_). The existence of this key in self.dep_by_depended_step
         establish the existence of a dependency between two processing steps.
        :param depend_on_name: The name of the ProcessingStep object that is depended on.
        :param type_: The type of dependencies, as defined in SLURM.
        :return: None
        """
        step_dependency = StepDependency(depend_on_name, type_)
        if step_dependency.key() not in self.dep_by_depended_step :
            self._dep_by_depended_step[step_dependency.key()] = step_dependency


    def add_job_dependency(self, job_key: KeyDict, slurm_ids: list, depend_on_name : str, type_: str = "afterok"):
        """
         Add a dependency between jobs of the depending (i.e., the ProcessingStep that has this Dependencies object;
         the child) and the dependent (the step specified by depend_on_name) steps.  Such dependency can be
         "non-instantiated" (i.e., the job has not been launched, so there is no SLURM ID attributed yet),
         in which case slurm_ids is None. When jobs start to be launched, these dependencies get instantiated by
         calling this function with non-None slurm_ids list.
        :param job_key: KeyDict objects specifying the values for the ProcessingStep candidate properties for the
                         dependent job.
        :param slurm_ids: Iterable of SLURM job ID that are depended on.
        :param depend_on_name: The name of the ProcessingStep object that is depended on.
        :param type_: The type of dependencies, as defined in SLURM.
        :return: None
        """

        # Fill self.dep_by_depended_step
        step_dependency_key = StepDependency(depend_on_name, type_).key()
        if step_dependency_key not in self.dep_by_depended_step:
            self.add_step_dependency(depend_on_name, type_)

        self.dep_by_depended_step[step_dependency_key].add_job_dependency(job_key, slurm_ids)

        # Fill self.dep_by_job_key
        job_key, slurm_ids = StepDependency.check_job_dependency_arguments(job_key, slurm_ids)

        if job_key in self.dep_by_job_key:
            if type_ in self.dep_by_job_key[job_key]:
                if slurm_ids is not None:
                    self.dep_by_job_key[job_key][type_].extend(slurm_ids)
            else:
                self.dep_by_job_key[job_key][type_] = slurm_ids
        else:
            self.dep_by_job_key[job_key] = {type_: slurm_ids}

    def get_all_depended_names(self):
        return np.unique([step_dependency.depend_on_name for step_dependency in self.dep_by_depended_step.values()]).tolist()

    def get_ids_for_key(self, job_key, type_=None):
        """
         Return a list of SLURM job IDS, given the key identifying a specific job within a ProcessingStep.
        :param job_key: Tuple specifying the levels for the different properties a ProcessingStep creates jobs for.
        :param type_: Type (as defined in SLURM) of the dependencies that should be returned. If None, return the
                      while dictionary, with dependency types as keys.
        :return:
        """
        if job_key not in self.dep_by_job_key:
            return []
        if type_ is None:
            return self.dep_by_job_key[job_key]
        return self.dep_by_job_key[job_key][type_]

    def get_dependency_strings(self, job_key):

        if job_key not in self.dep_by_job_key:
            return []

        dep_strings = []
        for type_, slurm_ids in self.dep_by_job_key[job_key]:
            dep_strings.append("--dependency={}:{}".format(type_, ":".join(np.array(slurm_ids, dtype=str))))

    def add_dependencies_for_job_key(self, job_key, dep_sup, name_from_slurm_id_func):
        if dep_sup is None:
            return

        if isinstance(dep_sup, (str, int)):
            dep_sup = [str(dep_sup)]

        if isinstance(dep_sup, dict):
            for type_, deps in dep_sup.items():
                if isinstance(deps, Iterable):
                    for dep in deps:
                        depended = name_from_slurm_id_func(dep)
                        self.add_job_dependency(job_key, [dep], depended, type_)
                else:
                    depended = name_from_slurm_id_func(deps)
                    self.add_job_dependency(job_key, [deps], depended, type_)

        elif isinstance(dep_sup, Iterable):
            for dep in dep_sup:
                depended = name_from_slurm_id_func(dep)
                self.add_job_dependency(job_key, [dep], depended, "afterok")

        else:
            raise TypeError
