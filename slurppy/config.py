import yaml
from pathlib import Path
from collections.abc import Mapping, MutableMapping, Collection
from tempfile import NamedTemporaryFile
import numpy as np
from warnings import warn
import sys


try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def yn_choice(message, default='y'):
    choices = 'Y[yes]/n[no)/a[abort]' if default.lower() in ('y', 'yes') else 'y[yes]/N[no)/a[abort]'
    choice = input("%s (%s) " % (message, choices))
    values = ('y', 'yes', '') if choices == 'Y/n' else ('y', 'yes')
    if choice.strip().lower() in ('a', 'abort'):
        raise RuntimeError("Execution aborted by the user.")
    return choice.strip().lower() in values


# Recursive updates. Default dictionary update is not recursive, which
# cause dict within dict to be simply overwritten rather than merged
def update(d, u):
    for k, v in u.items():
        dv = d.get(k, {})
        if not isinstance(dv, Mapping) and not hasattr(dv, "keys"):
            d[k] = v
        elif isinstance(v, Mapping) or hasattr(v, "keys"):
            d[k] = update(dv, v)
        else:
            d[k] = v
    return d


def join(loader, _, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


class SlurppyLoader(Loader):
    pass


SlurppyLoader.add_multi_constructor("!join", join)


class Config(MutableMapping):

    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.path = self.default_path()
        self.update(dict(*args, **kwargs))
        self._check_config_()

    def _check_config_(self):
        #config_changed = False

        for key in ["analysis", "slurm", "paths"]:
            if key not in self:
                self[key] = {}
            if self[key] is None:
                self[key] = {}

        if "email" not in self["slurm"]:
            self["slurm"]["email"] = ""
        if "send_emails" not in self["slurm"]:
            self["slurm"]["send_emails"] = False
        if "account" not in self["slurm"]:
            #if ask_account:
            #self["slurm"]["account"] = input("Enter a SLURM account: ")
            #config_changed = True
            self["slurm"]["account"] = ""

        if "venv_path" not in self["paths"]:
            self["paths"]["venv_path"] = str(Path(sys.executable).parent)
            warn("Your configuration file do not contain the key ['paths']['venv_path']. Defaulting to {}"
                 .format(self["paths"]["venv_path"]))
            config_changed = True
        #if config_changed:
        #    if yn_choice("You changed your configuration. Do you want to save the changes?", default='y'):
        #        if yn_choice("Save as the default configuration file (i.e., {})?".format(self.path),
        #                     default='y'):
        #            self.save()
        #    else:
        #        path = input("Enter the path where to save this new configuration file:")
        #        self.save(path)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __repr__(self):
        return self.store.__repr__()

    def __contains__(self, item):
        return item in self.store

    def __str__(self):
        return self.store.__str__()

    def pretty_print(self):
        print(yaml.dump(self.store, default_flow_style=False, default_style=''))

    def update(self, *args, **kwargs):
        if not args:
            raise TypeError("descriptor 'update' of 'Config' object "
                            "needs an argument")
        if len(args) > 1:
            raise TypeError('update expected at most 1 arguments, got {}'.format(len(args)))
        if args:
            update(self.store, args[0])
        update(self.store, kwargs)
        self._check_config_()

    @staticmethod
    def perso_default_path():
        return Path.home() / ".slurppy_config.yaml"

    @staticmethod
    def default_path():
        perso_default = Config.perso_default_path()
        if perso_default.exists():
            return perso_default
        return Path(__file__).parent.parent / "configs" / "default.yaml"

    @staticmethod
    def get_config(configs=(), load_default=True):
        """"
         Populate and return the configuration. It first use the default config as per self.default_path() if
         load_default is set to True. Then, it populates according the the items in configs. Since it
         process these items in turn, the fields of the last items can overshadow the previous items. Thus,
         if configs is an iterable, its order matters.
        """
        def _check_configs_type(configs_to_check):
            if isinstance(configs_to_check, (str, Path)):
                with Path(configs_to_check).open('r') as check_stream:
                    return [yaml.load(check_stream, Loader=SlurppyLoader)]
            if isinstance(configs_to_check, (dict, Config)):
                return [configs_to_check]
            if isinstance(configs_to_check, list):
                if len(configs_to_check) == 1:
                    return _check_configs_type(configs_to_check[0])
                if len(configs_to_check):
                    return np.concatenate([_check_configs_type(c) for c in configs_to_check]).tolist()
                return []
            if configs_to_check is None:
                return []
            raise TypeError("Received a configs object of an unrecognized type ({}).".format(type(configs_to_check)))

        self = Config()
        if load_default:
            print("Default config path: ", Config.default_path())
            self.path = Config.default_path()
            with self.path.open('r') as stream:
                self.update(yaml.load(stream, Loader=SlurppyLoader))

        configs = _check_configs_type(configs)
        for config in configs:
            self.update(config)

        # If load_default is true and additional config paths have been provided
        # the resulting config is a merging of more than a file so it cannot be loaded
        # from disk from child processes. Thus, we save it in a temporary file and keep
        # this path in memory. There is no automatic deletion of these temporary files because
        # there is no way to know up to when these files may be used.
        if load_default and len(configs):
            dir_ = None
            if "paths" in self.store:
                if "log_dir" in self.store["paths"]:
                    dir_ = self.store["paths"]["log_dir"]
                    try:
                        Path(self.store["paths"]["log_dir"]).mkdir(parents=True, exist_ok=True)
                    except PermissionError:
                        warn("The path specified in your configuration file in ['path']['log_dir']" +
                             "(i.e., {}) does not exist and could not be created."
                             .format(self.store["paths"]["log_dir"]))
                        dir_ = None
            temp_file = NamedTemporaryFile(mode='w+b', delete=False, prefix="slurppy_tmp_conf_",
                                           dir=dir_, suffix=".yaml")
            self.path = Path(temp_file.name)
            self.save()

        return self

    def save(self, path=None):
        if path is None:
            path = self.path
        if isinstance(path, str):
            path = Path(path)
        with path.open("w") as stream:
            yaml.dump(self.store, stream, Dumper=Dumper)
