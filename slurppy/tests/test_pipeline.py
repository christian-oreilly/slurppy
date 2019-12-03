from pathlib import Path
from slurppy import Pipeline, ProcessingStep, Campaign
from . import mocked_input


def dummy_func(dummy_arg):
    print(dummy_arg)


def test_save_dummy_pipeline(mocked_input):
    pipeline = Pipeline()

    pipeline.add_step(ProcessingStep(
        name="test_dummy_func1",
        candidate_properties=["dummy_arg_1"],
        fct_str="dummy_func",
        import_module="slurppy.tests.test_pipeline"
    ))
    pipeline.add_step(ProcessingStep(
        name="test_dummy_func2",
        candidate_properties=["dummy_arg_2"],
        fct_str="dummy_func",
        import_module="slurppy.tests.test_pipeline"
    ))
    pipeline.add_step(ProcessingStep(
        name="test_dummy_func3",
        candidate_properties=["dummy_arg_3"],
        fct_str="dummy_func",
        import_module="slurppy.tests.test_pipeline"
    ))

    pipeline["test_dummy_func2"].add_dependency("test_dummy_func1")
    pipeline["test_dummy_func3"].add_dependency("test_dummy_func2")

    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    path.parent.mkdir(parents=True, exist_ok=True)
    pipeline.save(path)


def test_ready_jobs(mocked_input):
    test_save_dummy_pipeline(mocked_input)
    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    pipeline = Pipeline().load(path)
    campaign = Campaign()
    campaign.pipeline = pipeline
    pipeline.ready_jobs(verbose=True)


def test_pipeline_show(mocked_input):
    test_save_dummy_pipeline(mocked_input)
    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    pipeline = Pipeline().load(path)
    pipeline.show(expand=False)
    pipeline.show(expand=True)


def test_to_json(mocked_input):
    test_save_dummy_pipeline(mocked_input)
    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    pipeline = Pipeline().load(path)
    pipeline.to_json()
    print(pipeline)
