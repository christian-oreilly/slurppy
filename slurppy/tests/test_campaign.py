from pathlib import Path
from slurppy import Pipeline, ProcessingStep, Campaign
import pytest
from shutil import which


#@pytest.mark.dependency(depends=['slurppy/slurppy/tests/test_pipeline.py::test_pipeline_add_step'], scope="session")
def test_pipeline_add_step():
    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    pipeline = Pipeline().load(path)

    campaign = Campaign(name="test_campaign")

    # To avoid input asked to the used during tests
    if "account" not in campaign.config["slurm"]:
        campaign.config["slurm"]["account"] = "dummy_account"
    if "venv_path" not in campaign.config["paths"]:
        campaign.config["paths"]["venv_path"] = "dummy_venv"

    campaign.set_workflow(pipeline, job_dependencies={})
    campaign.show_workflow()
    campaign.load_or_run(rerun=True, test=(which('sbatch') is None))
    #campaign.print_status()
