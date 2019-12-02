import subprocess
import numpy as np
from collections import OrderedDict
from pathlib import Path
import typing

if typing.TYPE_CHECKING:
    from .processingstep import ProcessingStep


class KeyDict(OrderedDict):

    def __repr__(self):
        return "{" + ", ".join(["{}: {}".format(key, val) for key, val in self.items()]) + "}"

    def __hash__(self):
        return hash(tuple(sorted(self.items())))

    def __contains__(self, item):
        for (key, value) in zip(item.keys(), item.values()):
            if (key, value) not in list(zip(self.keys(), self.values())):
                return False
        return True

    def __lt__(self, other):
        if self == other:
            return False
        for key1, key2 in zip(self.keys(), other.keys()):
            if key1 == key2:
                continue
            return key1 < key2
        for val1, val2 in zip(self.values(), other.values()):
            if val1 == val2:
                continue
            return val1 < val2

    def __gt__(self, other):
        return other < self

    def to_json(self):
        return [(key, val) for key, val in self.items()]


class Job:
    def __init__(self, processing_step, id_key, verbose=True, **kwargs):

        for key, val in kwargs.items():
            setattr(self, key, val)

        self.verbose: bool = verbose
        self.processing_step: ProcessingStep = processing_step
        self.id_key: KeyDict = id_key
        if len(id_key):
            name: str = Job.make_name(self.processing_step, id_key)
        else:
            name = self.processing_step.name
        self.name: str = name

        self.slurm_id: typing.Optional[str] = None
        self.file_name_slurm: typing.Optional[Path] = None
        self.file_name_log: typing.Optional[Path] = None

        self.set_file_names()

    @staticmethod
    def make_name(step, id_key: KeyDict):
        return "_".join(np.concatenate(([step.name], list(id_key.values()))))

    def set_file_names(self):
        if "slurm_dir" in self.processing_step.config["paths"]:
            slurm_path = Path(self.processing_step.config["paths"]["slurm_dir"])
        else:
            slurm_path = Path()
        slurm_path = (slurm_path / self.name).with_suffix(".sh")

        if "log_dir" in self.processing_step.config["paths"]:
            log_path = Path(self.processing_step.config["paths"]["log_dir"])
        else:
            log_path = Path()
        log_path = (log_path / self.name).with_suffix(".log")

        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError("The path specified in your configuration file in ['path']['log_dir']" +
                                  "(i.e., {}) does not exist and could not be created.".format(log_path))

        try:
            slurm_path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError("The path specified in your configuration file in ['path']['slurm_dir']" +
                                  "(i.e., {}) does not exist and could not be created.".format(slurm_path))

        self.file_name_slurm = slurm_path
        self.file_name_log = log_path

    @property
    def id_key(self):
        return self._id_key

    @id_key.setter
    def id_key(self, id_key: KeyDict):
        if not isinstance(id_key, KeyDict):
            raise TypeError("id_key must be of type KeyDict.")
        self._id_key = id_key

    def get_dependency_ids(self, job_key):
        return self.processing_step.get_dependency_ids(job_key)

    def cancel(self):
        subprocess.check_output(["scancel", str(self.slurm_id)])

    def print_status(self):
        print('{:<15}{:<70}{:<15}'.format(self.slurm_id,
                                          self.name,
                                          self.get_status()))

    def get_status(self):
        command = ["sacct", "-j", str(self.slurm_id), "--state=BF,CA,CD,DL,F,NF,OOM,PD,PR,R,RQ,RS,RV,S,TO",
                   "-X", "--format=State"]
        result = subprocess.check_output(command).decode().split()
        if len(result) >= 3:
            return result[2]
        else:
            return "UNKNOWN"

    def print_script(self):
        with self.file_name_slurm.open("r") as slurm_file:
            print(slurm_file.read())

    def print_info(self):
        print("#"*45 + " JOB INFO " + "#"*45)
        print("job name:", self.name)
        print("job key:", self.id_key)
        print("job id:", self.slurm_id)
        print("job depends on ids:", self.processing_step.get_dependency_ids(self.id_key))
        print("#"*100)
