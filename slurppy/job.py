
import subprocess
import sys
import numpy as np
from itertools import product
from collections import OrderedDict
from jinja2 import Template
from warnings import warn
from pathlib import Path

from .config import Config


def yn_choice(message, default='y'):
    choices = 'Y/n' if default.lower() in ('y', 'yes') else 'y/N'
    choice = input("%s (%s) " % (message, choices))
    values = ('y', 'yes', '') if choices == 'Y/n' else ('y', 'yes')
    return choice.strip().lower() in values


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


class Job:

    def __init__(self, job_name, processing_step, fct_kwargs=None, small=False, resume=True,
                 verbose=True, **kwargs):

        self.name = job_name
        self.small = small
        self.resume = resume
        self.verbose = verbose
        self.processing_step = processing_step

        for key, val in kwargs.items():
            setattr(self, key, val)

        if fct_kwargs is None:
            self.fct_kwargs = {}
        else:
            self.fct_kwargs = fct_kwargs

        self.job_ids = {}
        self.specific_names = {}
        self.file_names_slurm = {}
        self.file_names_log = {}
        self.properties = OrderedDict()

        self.generate_batch_scripts()

    @staticmethod
    def format_args(key, val):
        if isinstance(val, str):
            return "{}='{}'".format(key, val)
        else:
            return "{}={}".format(key, val)

    def _check_config_(self):
        config = self.processing_step.config

        config_changed = False
        if "email" not in config["slurm"]:
            config["slurm"]["email"] = ""
        if "send_emails" not in config["slurm"]:
            config["slurm"]["send_emails"] = False
        if "account" not in config["slurm"]:
            config["slurm"]["account"] = input("Enter a SLURM account: ")
            config_changed = True
        if "venv_path" not in config["paths"]:
            config["paths"]["venv_path"] = str(Path(sys.executable).parent)
            warn("Your configuration file do not contain the key ['paths']['venv_path']. Defaulting to {}"
                 .format(config["paths"]["venv_path"]))
            config_changed = True
        if config_changed:
            if yn_choice("You changed your configuration. Do you want to save the changes?", default='y'):
                if yn_choice("Save as the default configuration file (i.e., {})?".format(Config.perso_default_path()),
                             default='y'):
                    config.save(Config.perso_default_path())
            else:
                path = input("Enter the path where to save this new configuration file:")
                config.save(path)

    def generate_batch_scripts(self):
        self.properties = self.processing_step.get_loop_properties()

        for item in product(*self.properties.values()):
            job_key = KeyDict(((key, val) for key, val in zip(self.properties.keys(), item)))

            specific_job_name = "_".join(np.concatenate(([self.name], item)))
            self.specific_names[job_key] = specific_job_name

            # Formatting the python command
            kwargs = self.fct_kwargs.copy()
            kwargs["small"] = self.small
            kwargs["resume"] = self.resume
            if self.processing_step.config is not None:
                kwargs["config"] = self.processing_step.config
            kwargs.update(job_key)
            arg_str = [self.format_args(key, val) for key, val in kwargs.items()]
            python_code = 'from {module} import {fct}; {fct}({kwd})'.format(module=self.processing_step.import_module,
                                                                            fct=self.processing_step.fct_str,
                                                                            kwd=", ".join(arg_str))

            command = 'python -c "{}"'.format(python_code)

            self._check_config_()
            config = self.processing_step.config
            if self.name in config["slurm"]:
                config_task_root = config["slurm"][self.name]
            elif hasattr(self, "slurm_config_key"):
                config_task_root = config["slurm"][self.slurm_config_key]
            elif "default" in config["slurm"]:
                config_task_root = config["slurm"]["default"]
                warn("Using config key ['slurm']['default'] for {} since not entry was found for this task."
                     .format(self.name))
            else:
                config_task_root = {}
                warn("Using no SLURM configuration dictionnary for  {} because no entry was found for this task and" +
                     " there is no key ['slurm']['default'] currently defined in the configuration file used."
                     .format(self.name))

            if self.small:
                config_task_root = config_task_root["small"]

            if "slurm_dir" in config["paths"]:
                slurm_path = Path(config["paths"]["slurm_dir"])
            else:
                slurm_path = Path()
            slurm_path = (slurm_path / specific_job_name).with_suffix(".sh")

            if "log_dir" in config["paths"]:
                log_path = Path(config["paths"]["log_dir"])
            else:
                log_path = Path()
            log_path = (log_path / specific_job_name).with_suffix(".log")

            template_path = Path(__file__).parent.parent / "templates" / "slurm_template.jinja"
            with template_path.open("r") as file_jinja:
                jinja_template = file_jinja.read()

            kwargs = {"job_name": specific_job_name,
                      "email": config["slurm"]["email"],
                      "send_emails": config["slurm"]["send_emails"],
                      "file_name_log": str(log_path),
                      "account": config["slurm"]["account"],
                      "venv_path": config["paths"]["venv_path"],
                      "command": command}
            kwargs.update(config_task_root)

            slurm_script = Template(jinja_template).render(**kwargs)

            if self.verbose:
                print("Saving ", slurm_path)

            log_path.parent.mkdir(parents=True, exist_ok=True)
            slurm_path.parent.mkdir(parents=True, exist_ok=True)
            with slurm_path.open("w") as file_slurm:
                file_slurm.write(slurm_script)

            self.file_names_slurm[job_key] = slurm_path
            self.file_names_log[job_key] = log_path

    def run_a_job(self, job_key, verbose=None, test=False, dep_sup=None):
        args = ["sbatch"]
        dep_ids = self.processing_step.get_dependency_ids(job_key)
        if len(dep_ids):
            args.append("--dependency=afterok:" + ":".join(np.array(dep_ids, dtype=str)))
        if dep_sup is not None:
            if isinstance(dep_sup, (str, int)):
                args.append("--dependency=afterok:{}".format(dep_sup))
            elif isinstance(dep_sup, list):
                args.append("--dependency=afterok:" + ":".join(np.array(dep_sup, dtype=str)))
            elif isinstance(dep_sup, dict):
                for key, val in dep_sup.items():
                    args.append("--dependency={}:{}".format(key, val))
            else:
                raise TypeError
        args.append(str(self.file_names_slurm[job_key]))

        if verbose:
            print(" ".join(args))

        if test:
            print(" ".join(args))
            self.job_ids[job_key] = 99999999
        else:
            res = subprocess.check_output(args).strip()

            if verbose:
                print(res.decode(), file=sys.stdout)

            if not res.startswith(b"Submitted batch"):
                warn("There has been an error launching the job {}.".format(job_key))
                return

            self.job_ids[job_key] = int(res.split()[-1])
            return self.job_ids[job_key]

    def run(self, verbose=None, test=False):
        if verbose is None:
            verbose = self.verbose

        for item in product(*self.properties.values()):
            job_key = KeyDict(((key, val) for key, val in zip(self.properties.keys(), item)))
            self.run_a_job(job_key, verbose=verbose, test=test)

    def cancel(self):
        for job_id in self.job_ids.values():
            subprocess.check_output(["scancel", str(job_id)])

    @staticmethod
    def _get_job_status(job_id):
        command = ["sacct", "-j", str(job_id), "--state=BF,CA,CD,DL,F,NF,OOM,PD,PR,R,RQ,RS,RV,S,TO",
                   "-X", "--format=State"]
        result = subprocess.check_output(command).decode().split()
        if len(result) >= 3:
            return result[2]
        else:
            return "UNKNOWN"

    def get_status(self):
        return {self._get_job_status(job_id) for job_id in self.job_ids.values()}

    def print_status(self):
        for job_key in self.job_ids:
            print('{:<15}{:<70}{:<15}'.format(self.job_ids[job_key],
                                              self.specific_names[job_key],
                                              self._get_job_status(self.job_ids[job_key])))
