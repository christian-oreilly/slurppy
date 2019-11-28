import numpy as np
from collections import OrderedDict
import pickle
from pathlib import Path
import subprocess
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from .job import Job
from .config import Config


class Campaign:
    def __init__(self, small=False, resume=True, config=None, name="campaign",
                 pipeline=None, job_dependencies=None, verbose=True):
        self.small = small
        self.resume = resume
        self.jobs = OrderedDict()
        self.verbose = verbose
        self.name = name
        self._pipeline = pipeline

        if job_dependencies is None:
            self.job_dependencies = {}
        else:
            self.job_dependencies = job_dependencies

        self.config = config

        output_path = Path()
        if "paths" in self.config:
            if "output_root" in self.config["paths"]:
                output_path = Path(self.config["paths"]["output_root"])
        self.file_name = (output_path / self.name).with_suffix(".cpg")

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):

        if isinstance(config, str):
            config_paths = [Path(config)]
            load_default = False
        elif isinstance(config, Path):
            config_paths = [config]
            load_default = False
        elif isinstance(config, (Config, dict)) or config is None:
            config_paths = []
            load_default = True
        else:
            raise TypeError("Received a config object of an unrecognized type ({}).".format(type(config)))

        self._config = Config.get_config(config_paths=config_paths, load_default=load_default)

        if isinstance(config, (Config, dict)):
            self._config.update(config)

        if self.pipeline is not None:
            self.pipeline.config = self._config

        assert(self._config is not None)


    @property
    def pipeline(self):
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        self._pipeline = pipeline
        self._pipeline.config = self.config

    def set_workflow(self, pipeline=None, job_dependencies=None, verbose=None):
        if pipeline is None:
            return

        self.pipeline = pipeline
        self.pipeline.config = self.config

        if verbose is None:
            verbose = self.verbose

        if job_dependencies is not None:
            self.job_dependencies = job_dependencies

        for job_name, processing_step in self.pipeline.items():

            if job_name in self.job_dependencies:
                for job_dependency in self.job_dependencies[job_name]:
                    if job_dependency not in self.jobs:
                        raise ValueError(job_name + " depends on " + job_dependency +
                                         ", but no such job has been ran.")
                processing_step.dep_after = [self.jobs[job_dependency].job_ids
                                             for job_dependency in self.job_dependencies[job_name]]

            self.jobs[job_name] = Job(job_name=job_name, small=self.small,
                                      resume=self.resume, config=self.config,
                                      verbose=verbose, processing_step=processing_step)

    def run(self, include=None, exclude=None, test=False, verbose=None):
        if include is not None:
            exclude = [job_name for job_name in self.step_names if job_name not in include]
        elif exclude is None:
            exclude = []

        for job_name in self.jobs:
            if job_name not in exclude:
                self.jobs[job_name].run(verbose=verbose, test=test)

    def cancel(self):
        for job in self.jobs.values():
            job.cancel()

    def get_status(self):
        return {job.name: job.get_status() for job in self.jobs.values()}

    def print_status(self):
        for job in self.jobs.values():
            print("=" * int(np.ceil((100 - len(job.name) - 1) / 2.0)) + " " + job.name + " " +
                  "=" * int(np.floor((100 - len(job.name) - 1) / 2.0)))
            job.print_status()
            print(" ")

    def print_log(self, job_id, tail=None, head=None):
        for job in self.jobs.values():
            if job_id in job.job_ids.values():
                keys = list(job.job_ids.keys())
                values = list(job.job_ids.values())
                with open(job.file_names_log[keys[values.index(job_id)]], "r") as log_file:
                    log_text = log_file.read()
                if head is not None:
                    print("\n".join(log_text.split("\n")[:head]))
                elif tail is not None:
                    print("\n".join(log_text.split("\n")[-tail:]))
                else:
                    print(log_text)
                return

    def print_script(self, job_id):
        for job in self.jobs.values():
            if job_id in job.job_ids.values():
                keys = list(job.job_ids.keys())
                values = list(job.job_ids.values())
                with open(job.file_names_slurm[keys[values.index(job_id)]], "r") as slurm_file:
                    print(slurm_file.read())
                return

    def relaunch_job(self, job_id, dep_sup=None):
        for job in self.jobs.values():
            for job_key, id_ in job.job_ids.items():
                if job_id == id_:
                    new_id = job.run_a_job(job_key, dep_sup=dep_sup)
                    if new_id is not None:
                        print("Job {} launched.".format(new_id))

    def print_job_id_info(self, job_id):
        for job in self.jobs.values():
            for job_key, id_ in job.job_ids.items():
                if job_id == id_:
                    print("#"*45 + " JOB INFO " + "#"*45)
                    print("job name:", job.specific_names[job_key])
                    print("job key:", job_key)
                    print("job id:", job_id)
                    print("job depends on ids:", job.get_dependency_ids(job_key))
                    print("#"*100)

    def _check_file_name_(self, file_name):
        if file_name is not None:
            if isinstance(file_name, str):
                file_name = Path(file_name)
            self.file_name = file_name

    def save(self, file_name=None):
        self._check_file_name_(file_name)

        with self.file_name.open("wb") as f:
            pickle.dump(self, f)
        print("Saving campaign as {}.".format(self.file_name))

    def load(self, file_name=None):
        self._check_file_name_(file_name)
        with self.file_name.open("rb") as f:
            self = pickle.load(f)

        return self

    def load_or_run(self, rerun=False, file_name=None, **run_kwargs):
        """
         This commands load the campaign if it already exists (i.e., if its the file pointed to by
          file_name or self.file_name exists) and if rerun is False. Else, it run it and save it.
        :param rerun: Specify whether the campaign should be rerun if it already exists.
        :param file_name: Path where to save the campaign to or load the campaign from.
        :param run_kwargs: Arguments to be passed to the self.run() method.
        :return: None
        """

        if rerun:
            self.run(**run_kwargs)
            self.save(file_name)
            return

        try:
            self.load(file_name)
        except IOError:
            self.run(**run_kwargs)
            self.save(file_name)

    def show_workflow(self):
        block_test = "blockdiag {\n"

        for children in list(self.job_dependencies.keys()):
            for parent in self.job_dependencies[children]:
                block_test += "    {} -> {};\n".format(parent, children)
        block_test += "}\n"

        with open("diagram", "w") as f:
            f.write(block_test)

        subprocess.check_output(["blockdiag", "--size=2000x2000", "diagram"])

        fig, axes = plt.subplots(1, 1, figsize=(15, 15))
        img = mpimg.imread('diagram.png')
        axes.imshow(img)
        plt.axis('off')
