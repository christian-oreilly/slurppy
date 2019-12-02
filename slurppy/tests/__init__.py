import pytest


def mocked_input_for_check_config(prompt):
    if prompt == "Enter a SLURM account: ":
        return "some_account"
    if prompt == "You changed your configuration. Do you want to save the changes? (Y/n) ":
        return "y"
    if prompt[:38] == "Save as the default configuration file":
        return "y"

    raise ValueError("Not mocked prompted input: '{}'".format(prompt))


# from .test_pipeline import save_dummy_pipeline
# save_dummy_pipeline()


@pytest.fixture  # (autouse=True)
def mocked_input(monkeypatch):
    """Remove requests.sessions.Session.request for all tests."""
    monkeypatch.setattr('builtins.input', mocked_input_for_check_config)
