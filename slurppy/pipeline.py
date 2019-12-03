from collections import OrderedDict
from pathlib import Path
import pickle
import json
import subprocess
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
import typing
from warnings import warn
from configmng import Config
from tempfile import NamedTemporaryFile
import os

class Pipeline:
    def __init__(self, name="slurppy_pipeline"):
        self.processing_steps = OrderedDict()

        # The pipeline gets its config from the campaign when
        # they are associated with campaign.pipeline = pipeline
        self._config = None

        self.name = name
        self.file_name = None

    def __getitem__(self, key):
        return self.processing_steps[key]

    def __setitem__(self, key, value):
        self.processing_steps[key] = value

    def __delitem__(self, key):
        del self.processing_steps[key]

    def __iter__(self):
        return iter(self.processing_steps)

    def __len__(self):
        return len(self.processing_steps)

    def __contains__(self, item):
        return item in self.processing_steps

    def __repr__(self):
        return str(self.to_json())

    def __str__(self):
        return json.dumps(self.to_json(), indent=4, sort_keys=True)

    def to_json(self):
        return {self.name: {name: step.to_json() for name, step in self.processing_steps.items()}}

    def _check_pipeline_associated_with_campaing_(self):
        if self._config is None:
            raise ValueError("This operation cannot be performed before the pipeline is "
                             "associated with a campaign using 'campaign.pipeline = pipeline'.")

    @property
    def config(self):
        self._check_pipeline_associated_with_campaing_()
        return self._config

    @config.setter
    def config(self, config: typing.Optional[Config]):
        if config is None:
            return
        if not isinstance(config, Config):
            raise TypeError("config must be of type Config. Received type: {}".format(type(config)))

        self._config = config
        for step in self.processing_steps.values():
            step.config = config

    # def add_dependency(self, dependent, depended_on, type_):
    #    self._dependencies.add_dependency(dependent, depended_on, type_)

    def add_step(self, processing_step):
        if processing_step.name in self.processing_steps:
            msg = "There is always a processing step with this name ({name}) in the pipeline." + \
                  " If you really want to add this step, you will need first to remove the existing step" + \
                  " by calling Pipeline.remove_step({name})"
            raise ValueError(msg.format(name=processing_step.name))

        self.processing_steps[processing_step.name] = processing_step
        self.processing_steps[processing_step.name].pipeline = self

        if self._config is not None:
            self.processing_steps[processing_step.name].config = self.config

    def remove_step(self, name):
        del self.processing_steps[name]

    def items(self):
        return self.processing_steps.items()

    @property
    def step_names(self):
        return list(self.processing_steps.keys())

    def _check_file_name_(self, file_name):
        if file_name is None:
            if self.file_name is None:
                self.file_name = (Path(self.config["paths"]["output"]) / self.name).with_suffix(".pln")
        else:
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
            loaded_pipeline = pickle.load(f)

        self.processing_steps = loaded_pipeline.processing_steps
        self._config = loaded_pipeline._config
        self.name = loaded_pipeline.name
        self.file_name = loaded_pipeline.file_name
        return loaded_pipeline

    def run(self, include=None, exclude=None, test=False, verbose=None, slurm_account=""):

        if self.config["slurm"]["account"] == "" and slurm_account == "":
            warn("No values have been set for ['slurm']['account'] in the configuration file and " +
                 "no slurm_account values have been passed to Pipeline.run().")
        if include is not None:
            exclude = [step_name for step_name in self.step_names if step_name not in include]
        elif exclude is None:
            exclude = []

        for step_name in self.processing_steps:
            if step_name not in exclude:
                self.processing_steps[step_name].run_jobs(verbose=verbose, test=test)

    def print_status(self):
        for step in self.processing_steps.values():
            print("=" * int(np.ceil((100 - len(step.name) - 1) / 2.0)) + " " + step.name + " " +
                  "=" * int(np.floor((100 - len(step.name) - 1) / 2.0)))
            step.print_status()
            print(" ")

    def find_job_by_slurm_id(self, slurm_id):
        for step in self.processing_steps.values():
            job = step.get_job_by_slurm_id(slurm_id)
            if job is not None:
                return step, job
        return None, None

    def get_job_by_slurm_id(self, slurm_id):
        return self.find_job_by_slurm_id(slurm_id)[1]

    def relaunch_job(self, slurm_id, dep_sup=None):
        step, job = self.find_job_by_slurm_id(slurm_id)
        if step is None:
            raise ValueError("Job with SLURM ID {} not found.".format(slurm_id))
        return step.run_a_job(self, job.id_key, dep_sup=dep_sup)

    def ready_jobs(self, small=False, resume=True, verbose=None):
        self._check_pipeline_associated_with_campaing_()
        for step in self.processing_steps.values():
            step.ready_jobs(small, resume, verbose)

    def show(self, expand=False, figsize=(15, 15), resolution=200):
        block_test = "blockdiag {\n"

        if len(self.processing_steps) == 1:
            block_test += "    {};".format(list(self.processing_steps.values())[0].name)
        else:
            if expand:
                for child_step in self.processing_steps.values():
                    for dependency in child_step.get_step_dependencies():
                        parent_step = self.processing_steps[dependency.depend_on_name]
                        for job_key in dependency.job_dependencies:
                            child_job = child_step._jobs[job_key]
                            parent_job = parent_step.get_job_by_partial_key_matching(child_job)
                            block_test += "    {} -> {};\n".format(parent_job.name, child_job.name)
            else:
                for child, step in self.processing_steps.items():
                    for parent in step.get_all_depended_names():
                        block_test += "    {} -> {};\n".format(parent, child)

        block_test += "}\n"

        temp_file = NamedTemporaryFile()
        with open(temp_file.name, "w") as f:
            f.write(block_test)

        size = "--size={}x{}".format(resolution*figsize[0], resolution*figsize[1])
        subprocess.check_output(["blockdiag", size, temp_file.name])
        fig, axes = plt.subplots(1, 1, figsize=figsize)
        img = mpimg.imread(temp_file.name + '.png')
        axes.imshow(img)
        plt.axis('off')
        os.remove(temp_file.name + '.png')

    def get_status(self):
        status = {}
        for step in self.processing_steps.values():
            status.update(step.get_status())
        return status

    def get_step_name_from_slurm_id(self, slurm_id):
        raise NotImplementedError()

    def cancel(self):
        for step in self.processing_steps.values():
            step.cancel()
