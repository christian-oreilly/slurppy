from pathlib import Path

from slurppy import Config
from . import mocked_input


def test_get_config(mocked_input):

    config = Path(__file__).parent.parent.parent / "configs" / "default.yaml"
    # Testing that loading configs speficied as different types do not crash
    Config.get_config(config)
    Config.get_config(str(config))
    Config.get_config([config])
    Config.get_config([config, {"another_field":{"inner":"property"}}])
    Config.get_config([Config({"another_field":{"inner":"property"}})])
    Config.get_config(Config({"another_field":{"inner":"property"}}))



def test_config_update(mocked_input):

    config = Config.get_config([{"another_field":{"inner1":"property"}},
                                {"another_field":{"inner2":"property"}}])
    assert("inner1" in config["another_field"])
    assert("inner2" in config["another_field"])
