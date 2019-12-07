
import pickle
from pathlib import Path
from warnings import warn
import numpy as np
from configmng import ConfigMng, ConfigArg
import os

from .pipeline import Pipeline


class Campaign:
    def __init__(self,
                 small: bool = False,
                 resume: bool = True,
                 config=None,
                 name: str = "campaign",
                 path: str = None,
                 pipeline: Pipeline = None,
                 verbose: bool = False,
                 interactive = True):

        self.small: bool = small
        self.resume: bool = resume
        self.verbose: bool = verbose
        self.name: str = name

        if path is None:
            path = os.getcwd()
        self.path: Path = Path(path)

        self._pipeline: Pipeline = pipeline
        self.config_mng: ConfigMng = ConfigMng(interactive=interactive)
        self.config = config
        self.add_default_schemas()
        self.check_config()

    @property
    def file_name(self):
        return (self.path / self.name).with_suffix(".cpg")

    def to_json(self):
        return {"name": self.name,
                "resume": self.resume,
                "small": self.small,
                "pipeline": self._pipeline}

    def __repr__(self):
        return str(self.to_json())

    def __str__(self):
        str_ = "Campaing {name} (resume={resume}, small={small}),\n" + \
               "with a pipeline {pipeline_name} of {nb_steps} ({pipeline_step_keys})."
        str_ = str_.format(name=self.name,
                           resume=self.resume,
                           small=self.small,
                           pipeline_name=self._pipeline.name,
                           nb_steps=len(self._pipeline.processing_steps),
                           pipeline_step_keys=list(self._pipeline.processing_steps.keys()))
        return str_

    @property
    def config(self):
        return self.config_mng.config

    @staticmethod
    def get_default_schema():
        return {
            "application": Path(__file__).parent.parent / "schemas" / "app_default_schema.yaml",
            "project": Path(__file__).parent.parent / "schemas" / "project_default_schema.yaml",
            "user": Path(__file__).parent.parent / "schemas" / "user_default_schema.yaml"
        }

    def get_default_config(self):
        return {
            "application": Path(__file__).parent.parent / "configs" / "app_default_config.yaml",
            "project": self.path / (self.name + "_config.yaml"),
            "user": Path.home() / ".slurrpy_user_config.yaml"
        }

    @config.setter
    def config(self, config: ConfigArg):

        default_config = self.get_default_config()

        for config_path in default_config.values():
            config_path.touch(exist_ok=True)

        if config is None:
            config = []

        are_level_configs = np.all([key in self.config_mng.level_order for key in config])
        if isinstance(config, dict) and are_level_configs:
            for level_name in self.config_mng.level_order:
                if level_name not in config:
                    if level_name in default_config:
                        config[level_name] = default_config[level_name]

        else:

            if not isinstance(config, list):
                config = [config]
            config = {"instance": config}
            config.update(default_config)

        for level_name, level_config in config.items():
            self.config_mng.set_level_configs(level_config, level_name)

        if self.pipeline is not None:
            self.pipeline.config = self.config

    def add_default_schemas(self):
        for level_name, level_schemas in self.get_default_schema().items():
            self.config_mng.add_schema(level_schemas, level_name)

    def check_config(self):
        self.config_mng.validate(interactive=True)

    @property
    def pipeline(self):
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        self._pipeline = pipeline
        self._pipeline.config = self.config
        self._pipeline.ready_jobs(self.small, self.resume, self.verbose)

    def run(self, include=None, exclude=None, test=False, verbose=None):
        self._pipeline.run(include, exclude, test, verbose)

    def cancel(self):
        self.pipeline.cancel()

    def get_status(self):
        return self.pipeline.get_status()

    def print_status(self):
        self.pipeline.print_status()

    def print_log(self, job_id, tail=None, head=None):
        with open(self.get_job(job_id).file_name_log, "r") as log_file:
            log_text = log_file.read()
        if head is not None:
            print("\n".join(log_text.split("\n")[:head]))
        elif tail is not None:
            print("\n".join(log_text.split("\n")[-tail:]))
        else:
            print(log_text)

    def get_job(self, job_id):
        job = self.pipeline.get_job_by_slurm_id(job_id)
        if job is None:
            raise ValueError("Job if {} not found in the campaign.".format(job_id))
        return job

    def print_script(self, job_id):
        self.get_job(job_id).print_script()

    def relaunch_job(self, job_id, dep_sup=None):
        new_id = self.pipeline.relaunch_job(job_id, dep_sup)
        if new_id is not None:
            print("Job {} launched.".format(new_id))

    def print_job_id_info(self, job_id):
        self.get_job(job_id).print_info()

    def _check_file_name_(self, file_name):
        if file_name is not None:
            if isinstance(file_name, str):
                file_name = Path(file_name)
            self.file_name = file_name

    def save(self, file_name=None):
        self._check_file_name_(file_name)

        with self.file_name.open("wb") as f:
            self.config_mng.make_serializable()
            pickle.dump(self, f)
        print("Saving campaign as {}.".format(self.file_name))

    def load(self, file_name=None):
        self._check_file_name_(file_name)
        with self.file_name.open("rb") as f:
            loaded_campaign: Campaign = pickle.load(f)

        self.small = loaded_campaign.small
        self.resume = loaded_campaign.resume
        self.verbose = loaded_campaign.verbose
        self.name = loaded_campaign.name
        self._pipeline = loaded_campaign.pipeline
        self.config_mng = loaded_campaign.config_mng
        self.file_name = loaded_campaign.file_name

        return loaded_campaign

    def load_or_run(self, rerun=False, file_name=None, raise_error="warning", **run_kwargs):
        """
         This commands load the campaign if it already exists (i.e., if its the file pointed to by
          file_name or self.file_name exists) and if rerun is False. Else, it run it and save it.
        :param rerun: Specify whether the campaign should be rerun if it already exists.
        :param file_name: Path where to save the campaign to or load the campaign from.
        :param raise_error: If set to "warning" (default), raised errors while attempting to read the
                            pickled campaign object are catched and the campaign is reran, issuing a warning.
                            If it is set to False, no warning are issued. If set to True, the error is not catched.
        :param run_kwargs: Arguments to be passed to the self.run() method.
        :return: None
        """

        if rerun:
            self.run(**run_kwargs)
            self.save(file_name)
            return

        try:
            self.load(file_name)

        # Monkey-patching. This is not an error. It is just indicative that the file does not
        # exists and that the campaign should be ran.
        except IOError:
            self.run(**run_kwargs)
            self.save(file_name)

        except Exception as e:
            if raise_error is True:
                raise
            if raise_error == "warning":
                warn("A problem happenned while trying to load the saved campaign object. " +
                     "Running it anew and saving the resulting object. This can happen for various reasons. " +
                     "This can often be due by changes in the structure of the campaign class between the" +
                     " time the campaign was pickled and now. If you would prefer these errors to be raised instead, " +
                     "use Campaign.load_or_run(..., raise_error=True). Exception error message:\n" + str(e))
            self.run(**run_kwargs)
            self.save(file_name)

    def show_pipeline(self, expand=False, *args, **kwargs):
        self._pipeline.show(expand=expand, *args, **kwargs)
