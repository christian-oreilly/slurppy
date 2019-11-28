from pathlib import Path
from slurppy import Pipeline, ProcessingStep
import pytest


def dummy_func(dummy_arg):
    print(dummy_arg)


@pytest.mark.dependency(scope="session")
def test_pipeline_add_step():
    pipeline = Pipeline()

    pipeline.add_step(ProcessingStep(
        name="test_dummy_func",
        candidate_properties=["dummy_arg"],
        fct_str="dummy_func",
        import_module="slurppy.tests.test_pipeline"
    ))

    path = Path(__file__).parent / "test_artifacts" / "pipeline.pln"
    path.parent.mkdir(parents=True, exist_ok=True)
    pipeline.save(path)
