#!/usr/bin/env python

import importlib
from typing import List, Callable
from amlaidatatests.tests.base import AbstractBaseTest
import ibis
import yaml
import jinja2


function_registry: List[Callable] = []

variables = {
    "suffix": "1234"
}

def template_factory(template: str):
    jinga_html_template = jinja2.Template(template)
    for func in function_registry:
        jinga_html_template.globals[func.__name__] = func
    return jinga_html_template

def template_function(f):
    function_registry.append(f)

@template_function
def var(a: str) -> str:
    r = variables.get(a)
    if r is None:
        raise Exception("Variable not set")
    return r

def load_config():
    with open("models/schema.yml") as stream:
        try:
            return (yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)

connection = ibis.sqlite.connect("geography.db")

def get_table(t: str):
    r = template_factory(t)
    rdrd = r.render(suffix="var")
    table = connection.table(rdrd)
    return table

def get_test() -> AbstractBaseTest:
    my_module = importlib.import_module(name='.tests.unique_combination_of_columns', package='amlaidatatests')
    return my_module.test

if __name__ == "__main__":
    config = load_config()
    for source in config["sources"]:
        for table in (source["tables"]):
            table_o = get_table(table["name"])
            if tabletests := table.get("tests"):
                for test in tabletests:
                    for k, config in test.items():
                        print(config)
                        if k == 'dbt_utils.unique_combination_of_columns':
                            print(k)
                            
                            my_module.test(table_o, **config)
