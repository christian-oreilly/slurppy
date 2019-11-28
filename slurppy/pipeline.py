from collections import OrderedDict
from pathlib import Path
import pickle


class Pipeline:
    def __init__(self, name="slurppy_pipeline"):
        self.processing_steps = OrderedDict()
        self._config = {}
        self.name = name

        self.file_name = None

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        if config is None:
            return

        self._config = config
        for step in self.processing_steps.values():
            step.config = config

    def add_step(self, processing_step):
        self.processing_steps[processing_step.name] = processing_step
        self.processing_steps[processing_step.name].config = self.config

    def items(self):
        return self.processing_steps.items()

    @property
    def step_names(self):
        return list(self.processing_steps.keys())

    def _check_file_name_(self, file_name):
        if file_name is None:
            if self.file_name is None:
                self.file_name = (Path(self.config["paths"]["output_root"]) / self.name).with_suffix(".pln")
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
            self = pickle.load(f)

        return self