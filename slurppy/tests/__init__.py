import pytest
from pathlib import Path


def mocked_input_for_check_config(prompt):
    if prompt == "Enter a SLURM account: ":
        return "some_account"
    if prompt == "You changed your configuration. Do you want to save the changes? (Y/n) ":
        return "y"
    if prompt[:38] == "Save as the default configuration file":
        return "y"

    config_paths = ["log", "slurm", "tmp", "output", "venv"]
    for path in config_paths:
        msg = "The key {} at path /paths of the configuration file".format(path)
        if prompt[:len(msg)] == msg:
            return str(Path(__file__).parent / "test_artifacts")
    for path in config_paths:
        msg = "The value  for the configuration key /paths/{} is not compatible with the pattern".format(path)
        if prompt[:len(msg)] == msg:
            return str(Path(__file__).parent / "test_artifacts")

    msg = "The key account at path /slurm of the configuration file"
    if prompt[:len(msg)] == msg:
        return "dummy_account"

    msg = "The key send_emails at path /slurm of the configuration file "
    if prompt[:len(msg)] == msg:
        return False

    raise ValueError("Not mocked prompted input: '{}'".format(prompt))


# from .test_pipeline import save_dummy_pipeline
# save_dummy_pipeline()


@pytest.fixture  # (autouse=True)
def mocked_input(monkeypatch):
    """Remove requests.sessions.Session.request for all tests."""
    monkeypatch.setattr('builtins.input', mocked_input_for_check_config)
