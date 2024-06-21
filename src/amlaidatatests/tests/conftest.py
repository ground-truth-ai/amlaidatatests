from amlaidatatests.config import ConfigSingleton, DatatestConfig
from omegaconf import OmegaConf
from xdist.workermanage import WorkerController
import argparse
import json
from dataclasses import fields

pytest_plugins = [
   "amlaidatatests.tests.fixtures.fixtures",
]

STRUCTURED_CONF = OmegaConf.structured(DatatestConfig)


class IngestConfigAction(argparse.Action):
    def __init__(self,
                 option_strings,
                 dest,
                 default=None,
                 required=False,
                 help=None,
                 ):

        super().__init__(option_strings=option_strings, dest=dest, default=default, required=required, help=help)


    def __call__(self, parser, namespace, values, option_string=None):
        current_conf = ConfigSingleton.get()
        if option_string == '--conf':
            conf = OmegaConf.load(values)
            conf = OmegaConf.merge(STRUCTURED_CONF, current_conf, conf)
        else:
            # Todo: recursive parsing?
            conf_for_param = {
                option_string.replace("--", ""): values
            }
            conf = OmegaConf.merge(STRUCTURED_CONF, current_conf, conf_for_param)
        ConfigSingleton().set_config(conf)

    def format_usage(self) -> str:
        return ' | '.join(self.option_strings)

def pytest_addoption(parser) -> None:
    # Use also to initialize the configuration singleton
    ConfigSingleton().set_config(STRUCTURED_CONF)
    parser.addoption(f"--conf", action=IngestConfigAction)
    for field in fields(DatatestConfig):
        parser.addoption(f"--{field.name}", action=IngestConfigAction, default=field.default)

def pytest_configure(config):
    _cfg = ConfigSingleton().get()
    for field in fields(DatatestConfig):
        getattr(_cfg, field.name)