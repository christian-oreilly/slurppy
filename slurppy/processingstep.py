import numpy as np
import subprocess
from collections import OrderedDict
from collections.abc import Iterable
import sys
from itertools import product
from jinja2 import Template
from warnings import warn
from pathlib import Path
import typing
from configmng import Config

from .dependencies import Dependencies
from .job import Job, KeyDict
from .pipeline import Pipeline


class ProcessingStep:

    def __init__(self,
                 name: str,
                 candidate_properties: Iterable,
                 fct_str: str,
                 import_module: str,
                 constraints: typing.Optional[Iterable] = None,
                 slurm_config_key: typing.Optional[str] = None,
                 fct_kwargs: typing.Optional[dict] = None):
        """
        :param name: A name for this processing step.
        :param candidate_properties: List of properties for which values will be taken from the configuration
                                     file. Jobs will be started for every combination of values of these properties.
        :param fct_str: Name of the function to call.
        :param import_module: The name of a module, so that the instruction "from import_module import fct_str"
                              will allow to import the function specified in fct_str.
        :param constraints: Dictionnary of constraints, establishing specific values for some of the
                            candidate_properties. The targeted property is specified as the key. The values
                            can be an iterable, in which case the property must be equals to one of the
                            items of the iterable. If the value is not iterable, the property must be
                            equal to that value.
        :param slurm_config_key: Key to find this configuration of the slurm jobs associated with this step. It will
                                 be search for in the configuration at ["slurm"][slurm_config_key]. When no
                                 slurm_config_key are specified, the name of the ProcessingStep is used as a
                                 slurm configuration key.
        """
        self.name: str = name
        self._config: Config = None
        self.slurm_config_key: str = slurm_config_key

        self.candidate_properties: Iterable = sorted(candidate_properties)

        self.fct_str: str = fct_str
        self.import_module: str = import_module
        if constraints is None:
            constraints = {}
        self.constraints: dict = constraints
        self._dependencies: Dependencies = Dependencies()

        if fct_kwargs is None:
            fct_kwargs = {}
        self.fct_kwargs: dict = fct_kwargs
        self._jobs: dict = {}

        self._config_task_root: typing.Optional[dict] = None
        self.pipeline: typing.Optional[Pipeline] = None

    def to_json(self):
        json_ = {"name": self.name,
                 "fct_str": self.fct_str,
                 "import_module": self.import_module}

        if len(self.constraints):
            json_["constraints"] = self.constraints

        if len(self._dependencies.dep_by_depended_step):
            json_["dependencies"] = self._dependencies.to_json()

        for attribute in ["candidate_properties", "slurm_config_key"]:
            if getattr(self, attribute) is not None:
                json_[attribute] = getattr(self, attribute)

        return json_

    def __repr__(self):
        return str(self.to_json())

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config: Config):
        if config is None:
            return

        if not isinstance(config, Config):
            raise TypeError("config must be of type Config. Received type: {}".format(type(config)))

        self._config = config

    def get_dependency_ids(self, job_key, type_=None):
        return self._dependencies.get_ids_for_key(job_key, type_)

    def get_job_by_slurm_id(self, slurm_id):
        for job in self._jobs.values():
            if job.slurm_id == slurm_id:
                return job
        return None

    def run_jobs(self, verbose=None, test=False):
        for job_key in self._jobs:
            self.run_a_job(job_key, verbose=verbose, test=test)

    def cancel(self):
        for job in self._jobs.values():
            job.cancel()

    def get_status(self):
        return {job.name: job.get_status() for job in self._jobs.values()}

    def run_a_job(self, job_key, verbose=None, test=False, dep_sup=None):

        if self.pipeline is None:
            raise RuntimeError("The pipeline attribute of this steps has not been set. " +
                               "This happens if the processing step has not been added to any pipeline.")
        args = ["sbatch"]

        self._dependencies.add_dependencies_for_job_key(job_key, dep_sup,
                                                        self.pipeline.get_step_name_from_slurm_id)

        dep_strings = self._dependencies.get_dependency_strings(job_key)
        for dep_str in dep_strings:
            args.append(dep_str)

        job = self._jobs[job_key]
        args.append(str(job.file_name_slurm))

        if verbose:
            print(" ".join(args))

        if test:
            print(" ".join(args))
            job.slurm_id = 99999999
        else:
            res = subprocess.check_output(args).strip()

            if verbose:
                print(res.decode(), file=sys.stdout)

            if not res.startswith(b"Submitted batch"):
                warn("There has been an error launching the job {}.".format(job_key))
                return

            job.slurm_id = int(res.split()[-1])

        return job.slurm_id

    def get_all_depended_names(self):
        return self._dependencies.get_all_depended_names()

    def get_step_dependencies(self):
        return self._dependencies.dep_by_depended_step.values()

    def ready_jobs(self, small=False, resume=True, verbose=None):
        job_keys = self.get_job_keys()
        for job_key in job_keys:
            self._jobs[job_key] = Job(id_key=job_key, verbose=verbose, processing_step=self)
            self.instanciate_job_dependencies(self._jobs[job_key], none_ok=True)
        if len(job_keys) == 0:
            self._jobs[KeyDict()] = Job(id_key=KeyDict(), verbose=verbose, processing_step=self)
        self.generate_batch_scripts(small=small, resume=resume)

    def get_job_by_partial_key_matching(self, child_job: Job):
        """
         A child specified by a job key will have a partial match to its parents because
         the child is generally defined by a superset of the properties of the parent. Find in
         self (the parent) the job_id with the properties subset that fits the child_job_key superset
         of properties.
        :param child_job: A job from of child step for which we look in the parent step (self) the corresponding job.
        :return: Job from the children step of self, which corresponding key_job values.
        """
        if not isinstance(child_job, Job):
            raise TypeError("child_job must be of type Job. Received type: {}".format(type(child_job)))

        parent_job_key = KeyDict([(property_name, child_job.id_key[property_name])
                                  for property_name in self.candidate_properties])
        if parent_job_key not in self._jobs:
            depended_name = Job.make_name(self, parent_job_key)
            raise ValueError("{} depends on {}".format(child_job.name, depended_name) +
                             ", but no such job has been ran. Make sure to add steps to " +
                             "the pipeline in order they depend on each other.")
        return self._jobs[parent_job_key]

    def get_slurm_id_by_partial_key_matching(self, child_job: Job):
        return self.get_job_by_partial_key_matching(child_job).slurm_id

    def instanciate_job_dependencies(self, job, none_ok=False):
        """
        :param job:
        :param none_ok: Jobs are readied before being ran. At that time, we want to populate
                        the dependencies at the job level (rather than just at the step level),
                        for example to be able to draw the expanded pipeline.
                        However, since the job have not been ran yet, not SLURM ID is available.
                        At that time, this function can be called with none_ok==True.
        :return: None.
        """
        for dependency in self._dependencies.dep_by_depended_step.values():
            depended_step = self.pipeline.processing_steps[dependency.depend_on_name]
            depended_slurm_id = depended_step.get_slurm_id_by_partial_key_matching(job)
            if depended_slurm_id is None and not none_ok:
                raise RuntimeError("The corresponding job ({}) has not been initiated yet.".format(job.name))
            self._dependencies.add_job_dependency(job.id_key, depended_slurm_id,
                                                  dependency.depend_on_name, dependency.type_)

    def get_job_keys(self):
        properties = OrderedDict()

        # For the properties which levels are specified in the configuration file
        updated_candidate_list = []
        print(self.config)
        for property_name in self.candidate_properties:
            if property_name in self.config["analysis"]:
                properties[property_name] = self.config["analysis"][property_name]
                updated_candidate_list.append(property_name)
            else:
                warn("There was not ['analysis']['{}'] in the configuration to specify ".format(property_name) +
                     "the levels of the corresponding candidate_properties for the step {}. ".format(self.name) +
                     "Removing the property from the candidate properties.")
        self.candidate_properties = updated_candidate_list

        # For the properties which levels come from parent steps
        for parent_step_name in self._dependencies.get_all_depended_names():
            parent_step = self.pipeline.processing_steps[parent_step_name]
            for parent_job_key in parent_step._jobs:
                for property_, level in parent_job_key.items():
                    if property_ not in properties:
                        properties[property] = [level]
                    elif level not in properties[property_]:
                        properties[property_].append(level)

        # Filtering the properties given the specified constraints
        for property_name in properties:
            if property_name in self.constraints:
                if isinstance(self.constraints[property_name], str):
                    self.constraints[property_name] = [self.constraints[property_name]]
                filtered_property_values = [p for p in properties[property_name]
                                            if p in self.constraints[property_name]]
                properties[property_name] = filtered_property_values

        if len(properties) == 0:
            return []

        property_names = list(properties.keys())
        job_keys = []
        for property_values in product(*properties.values()):
            job_keys.append(KeyDict([(name, value) for name, value in zip(property_names, property_values)]))

        return job_keys

    def get_python_command(self, job, small=False, resume=True):
        def format_args(key, val):
            if isinstance(val, str):
                return "{}='{}'".format(key, val)
            else:
                return "{}={}".format(key, val)

        kwargs = self.fct_kwargs.copy()
        kwargs["small"] = small
        kwargs["resume"] = resume
        if self.config is not None:
            kwargs["config"] = self.config
        kwargs.update(job.id_key)
        arg_str = [format_args(key, val) for key, val in kwargs.items()]
        python_code = 'from {module} import {fct}; {fct}({kwd})'.format(module=self.import_module,
                                                                        fct=self.fct_str,
                                                                        kwd=", ".join(arg_str))
        return 'python -c "{}"'.format(python_code)

    def generate_batch_scripts(self, small=False, resume=True, verbose=False):

        for job in self._jobs.values():
            kwargs = {"job_name": self.name,
                      "email": self.config["slurm"]["email"],
                      "send_emails": self.config["slurm"]["send_emails"],
                      "file_name_log": str(job.file_name_log),
                      "account": self.config["slurm"]["account"],
                      "venv_path": self.config["paths"]["venv"],
                      "command": self.get_python_command(job, small, resume)}

            if small:
                kwargs.update(self.get_config_task_root()["small"])
            else:
                kwargs.update(self.get_config_task_root())

            template_path = Path(__file__).parent.parent / "templates" / "slurm_template.jinja"
            with template_path.open("r") as file_jinja:
                slurm_script = Template(file_jinja.read()).render(**kwargs)

            if verbose:
                print("Saving ", job.file_name_slurm)

            with job.file_name_slurm.open("w") as file_slurm:
                file_slurm.write(slurm_script)

    def add_dependency(self, depend_on_name, verbose=True, *args, **kwargs):
        if verbose:
            print("Dependency added: {}->{}".format(self.name, depend_on_name))

        self._dependencies.add_step_dependency(depend_on_name, *args, **kwargs)

        # Update the candidate properties that are specific to this step with
        # the candidate properties of the steps it is dependent on.
        self.candidate_properties.extend(self.pipeline.processing_steps[depend_on_name].candidate_properties)
        self.candidate_properties = sorted(np.unique(self.candidate_properties).tolist())

    def print_status(self):
        for job in self._jobs.values():
            job.print_status()

    def get_config_task_root(self):
        if self._config_task_root is None:
            if self.name in self.config["slurm"]:
                self._config_task_root = self.config["slurm"][self.name]
            elif self.slurm_config_key is not None and self.slurm_config_key in self.config["slurm"]:
                self._config_task_root = self.config["slurm"][self.slurm_config_key]
            elif "default" in self.config["slurm"]:
                self._config_task_root = self.config["slurm"]["default"]
                warn("Using config key ['slurm']['default'] for {} since not entry was found for this task."
                     .format(self.name))
            else:
                self._config_task_root = {}
                warn("Using no SLURM configuration dictionnary for  {} because no entry was found for this task and" +
                     " there is no key ['slurm']['default'] currently defined in the configuration file used."
                     .format(self.name))

            if self.slurm_config_key is not None and self.slurm_config_key not in self.config["slurm"]:
                warn("A slurm_config_key has been provided but this key was not found in config['slurm']. " +
                     "Current keys in config['slurm] are {}.".format(list(self.config["slurm"].keys())))

        return self._config_task_root
