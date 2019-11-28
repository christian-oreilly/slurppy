from yaml import load, dump
from pathlib import Path
import collections

# python 3.8+ compatibility
try:
    collectionsAbc = collections.abc
except ImportError:
    collectionsAbc = collections

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Recursive updates. Default dictionnary update is not recursive, which
# cause dict within dict to be simply overwritten rather than merged
def update(d, u):
    for k, v in u.items():
        dv = d.get(k, {})
        if not isinstance(dv, collectionsAbc.Mapping):
            d[k] = v
        elif isinstance(v, collectionsAbc.Mapping):
            d[k] = update(dv, v)
        else:
            d[k] = v
    return d


def join(loader, tag_suffix, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


class SlurppyLoader(Loader):
    pass


SlurppyLoader.add_multi_constructor("!join", join)


class Config(collections.MutableMapping):

    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))

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

    def update(self, u):
        update(self.store, u)

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
    def get_config(config_paths=(), load_default=True):
        self = Config()

        if load_default:
            self.path = Config.default_path()
            with self.path.open('r') as stream:
                self.update(load(stream, Loader=SlurppyLoader))

        if isinstance(config_paths, str):
            config_paths = [config_paths]
        for config_path in config_paths:
            with Path(config_path).open('r') as stream:
                config_supp = load(stream, Loader=SlurppyLoader)
            if config_supp is not None:
                self.update(config_supp)

        return self

    def save(self, path=None):
        if path is None:
            path = Config.default_path()
        if isinstance(path, str):
            path = Path(path)
        with path.open("w") as stream:
            dump(self.store, stream, Dumper=Dumper)
