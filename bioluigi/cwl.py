"""
This module provide utilities for representing a Luigi workflow with Common
Workflow Language.
"""

import argparse
import inspect
import sys
from os.path import join
from subprocess import check_output
from warnings import warn

import bioluigi
import luigi
import yaml
from luigi.cmdline_parser import CmdlineParser
from luigi.contrib.external_program import ExternalProgramTask
from luigi.task import flatten

def gen_command_line_tool(task):
    return {'class': 'CommandLineTool',
            'baseCommand': task.program_args()[0],
            'arguments': task.program_args()[1:]}

def gen_workflow_inputs(task):
    return [{'type': 'File', 'location': inp.path} for inp in flatten(task.input())]

def gen_workflow_outputs(task):
    return [{'type': 'File', 'location': out.path} for out in flatten(task.output())]

def gen_inputs(task):
    inputs = []
    for inp in flatten(task.input()):
        if isinstance(inp, luigi.LocalTarget):
            inputs.append(inp.path)
        else:
            inputs.append({'id': repr(inp)})
    return inputs

def gen_outputs(task):
    outputs = []
    for out in flatten(task.output()):
        if isinstance(out, luigi.LocalTarget):
            outputs.append(out.path)
        else:
            outputs.append({'id': repr(out)})
    return outputs

def gen_workflow_step(task):
    workflow_step = {'id': repr(task),
                     'in': gen_inputs(task),
                     'out': gen_outputs(task)}
    if isinstance(task, ExternalProgramTask):
        workflow_step['run'] = gen_command_line_tool(task)
    else:
        workflow_step['run'] = inspect.getsource(task.run)
    return workflow_step

def gen_workflow(goal_task):
    """
    Produce a CWL representation of the given task.
    """
    workflow = {'cwlVersion': 'v1.1',
                'class': 'Workflow',
                'inputs': [],
                'outputs': gen_workflow_outputs(goal_task),
                'steps': []}

    # walk the workflow top to bottom
    fringe = [goal_task]

    # set of already emitted workflow steps identifiers
    emitted_steps = set()

    while fringe:
        t = fringe.pop()

        workflow_step = gen_workflow_step(t)

        if not workflow_step['id'] in emitted_steps:
            workflow['steps'].insert(0, workflow_step)
            emitted_steps.add(workflow_step['id'])

        # leaf tasks might have some defined inputs although no specific
        # dependencies
        if not t.requires():
            workflow['inputs'].extend([inp for inp in gen_workflow_inputs(t) if inp not in workflow['inputs']])

        # static dependencies
        for dep in luigi.task.flatten(t.requires()):
            fringe.append(dep)

        # dynamic dependencies
        if inspect.isgeneratorfunction(t.run):
            if all(d.complete() for d in luigi.task.flatten(t.requires())):
                for chunk in t.run():
                    for dep in luigi.task.flatten(chunk):
                        fringe.append(dep)
            else:
                warn('Not checking {} for dynamic dependencies: it is not satisfied.'.format(repr(t)))

    return workflow

def main():
    parser = argparse.ArgumentParser(description='Generate a CWL representation of a Luigi workflow')
    parser.add_argument('--output', default=sys.stdout)
    args, remaining_args = parser.parse_known_args(sys.argv[1:])
    root_task = CmdlineParser(remaining_args).get_task_obj()
    yaml.dump(gen_workflow(root_task), args.output)
