
from collections.abc import Iterable
import pickle
from pathlib import Path
from warnings import warn
import typing

from .config import Config
from .pipeline import Pipeline


class Campaign:
    def __init__(self,
                 small: bool = False,
                 resume: bool = True,
                 config: typing.Optional[Config] = None,
                 name: str = "campaign",
                 pipeline: Pipeline = None,
                 verbose: bool = False):

        self.small: bool = small
        self.resume: bool = resume
        self.verbose: bool = verbose
        self.name: str = name
        self._pipeline: Pipeline = pipeline
        self.config: typing.Optional[Config] = config

        output_path = Path()
        if "paths" in self.config:
            if "output_root" in self.config["paths"]:
                output_path = Path(self.config["paths"]["output_root"])
        self.file_name: Path = (output_path / self.name).with_suffix(".cpg")

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
        return self._config

    @config.setter
    def config(self, config):
        def _check_config_type(config_to_check):
            if isinstance(config_to_check, (dict, Config, Path)):
                return [config_to_check]
            if isinstance(config, Iterable):
                return [_check_config_type(c) for c in config_to_check]
            if isinstance(config_to_check, str):
                return [Path(config_to_check)]
            if config_to_check is None:
                return []
            raise TypeError("Received a config object of an unrecognized type ({}).".format(type(config_to_check)))

        config = _check_config_type(config)
        self._config = Config.get_config(configs=config)  # , load_default=load_default)

        # if isinstance(config, (Config, dict)):
        #    self._config.update(config)

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
            pickle.dump(self, f)
        print("Saving campaign as {}.".format(self.file_name))

    def load(self, file_name=None):
        self._check_file_name_(file_name)
        with self.file_name.open("rb") as f:
            loaded_campaign = pickle.load(f)

        self.small = loaded_campaign.small
        self.resume = loaded_campaign.resume
        self.verbose = loaded_campaign.verbose
        self.name = loaded_campaign.name
        self._pipeline = loaded_campaign.pipeline
        self.config = loaded_campaign.config
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
