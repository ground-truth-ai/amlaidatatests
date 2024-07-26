"""Singleton function, derived from the hydra project"""

# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
#
# MIT License
#
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# The contents of this file were substantially derived from
# https://github.com/facebookresearch/hydra/blob/main/hydra/core/singleton.py
# licensed under the MIT license. This constructs an omegaconf singleton which
# is common across the entire datatest. It is used to avoid passing the
# configuration across the entire codebase.

from copy import deepcopy
from typing import Any, Dict

from omegaconf.basecontainer import BaseContainer


class Singleton(type):
    """The singleton persists omegaconf configuration into a single,
    instancewide configuration
    """

    _instances: Dict[type, "Singleton"] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def instance(cls: Any, *args: Any, **kwargs: Any) -> Any:
        return cls(*args, **kwargs)

    @staticmethod
    def get_state() -> Any:
        instances = deepcopy(Singleton._instances)

        return {
            "instances": instances,
            # Resolvers have to be handled seperately.
            "omegaconf_resolvers": deepcopy(BaseContainer._resolvers),
        }

    @staticmethod
    def set_state(state: Any) -> None:
        Singleton._instances = state["instances"]

        BaseContainer._resolvers = deepcopy(state["omegaconf_resolvers"])
