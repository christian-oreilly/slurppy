from pathlib import Path
from slurppy import Pipeline, Campaign
from shutil import which
from configmng import Config

from . import mocked_input
from .test_pipeline import test_save_dummy_pipeline


def test_pipeline_add_step(mocked_input):
    test_save_dummy_pipeline(mocked_input)
    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    pipeline = Pipeline().load(path)

    campaign = Campaign(name="test_campaign",
                        config=Config({"analysis": {"dummy_arg_2": ["level1", "level2", "level3"],
                                                    "dummy_arg_3": ["sublevel1", "sublevel2"]}}))

    campaign.pipeline = pipeline
    assert(id(pipeline) == id(campaign.pipeline))

    assert(len(campaign.pipeline.processing_steps["test_dummy_func1"]._jobs) == 1)
    assert(len(campaign.pipeline.processing_steps["test_dummy_func2"]._jobs) == 3)

    campaign.show_pipeline()
    campaign.load_or_run(rerun=True, test=(which('sbatch') is None))

    print(campaign.pipeline)

    if which('sacct') is not None:
        campaign.print_status()



def test_no_config(mocked_input):
    Campaign(name="test_campaign")


def test_invalid_config(mocked_input):
    campaign = Campaign(name="test_campaign",
                        config={"instance_configs":[],
                                "user_config_path":{},
                                "project_config_path": {},
                                "application_config_path":{}})
