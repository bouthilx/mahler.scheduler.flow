# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.flow.resources -- TODO
===================================

.. module:: resources
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
import getpass
import logging
import os
import subprocess

from mahler.core.resources import Resources


logger = logging.getLogger(__name__)


SUBMISSION_ROOT = os.environ['FLOW_SUBMISSION_DIR']

FLOW_OPTIONS_TEMPLATE = "{array}time=2:59:00;job-name={job_name}"

FLOW_TEMPLATE = "flow-submit {container} --config {file_path} --options {options}"

COMMAND_TEMPLATE = "mahler execute{container}{tags}{options}"

SUBMIT_COMMANDLINE_TEMPLATE = "{flow} launch {command}"


class FlowResources(Resources):
    """
    """

    def __init__(self, max_workers=100):
        """
        """
        self.max_workers = max_workers

    def available(self):
        """
        """
        command = 'squeue -r -o %t -u {user}'.format(user=getpass.getuser())
        out = subprocess.check_output(command.split(" "))
        out = str(out, encoding='utf-8')
        states = dict()
        for line in out.split("\n")[1:]:  # ignore `ST` header
            line = line.strip()
            if not line:
                continue

            if line not in states:
                states[line] = 0

            states[line] += 1

        logger.debug('Nodes availability')
        for state, number in sorted(states.items()):
            logging.debug('{}: {}'.format(state, number))

        total_jobs = sum(number for name, number in states.items() if name != 'CG')

        logging.debug('total: {}'.format(total_jobs))

        return max(self.max_workers - total_jobs, 0)

    def submit(self, tasks, container=None, tags=tuple(), working_dir=None):
        """
        """
        nodes_available = self.available()
        logger.info('{} nodes available'.format(nodes_available))
        if not nodes_available:
            return
        array_option = 'array=1-{};'.format(min(len(tasks), nodes_available))
        flow_options = FLOW_OPTIONS_TEMPLATE.format(
            array=array_option, job_name=".".join(sorted(tags)))

        resources = []
        for name, value in tasks[0]['facility']['resources'].items():
            if name == 'cpu':
                resources.append('cpus-per-task={}'.format(value))
            elif name == 'gpu':
                resources.append('gres=gpu:{}'.format(value))
            elif name == 'mem':
                resources.append('mem={}'.format(value))
            else:
                raise ValueError('Unknown option: {}'.format(name))

        flow_options += ";".join(resources)

        submission_dir = os.path.join(SUBMISSION_ROOT, container)
        if not os.path.isdir(submission_dir):
            os.makedirs(submission_dir)

        if tags:
            file_name = ".".join(tag for tag in sorted(tags) if tag) + ".sh"
        else:
            file_name = "all.sh"

        file_path = os.path.join(submission_dir, file_name)

        flow_command = FLOW_TEMPLATE.format(
            container=container, file_path=file_path, options=flow_options)

        command = COMMAND_TEMPLATE.format(
            container=" --container " + container if container else "",
            tags=" --tags " + " ".join(tags) if tags else "",
            options=" --working-dir={}".format(working_dir) if working_dir else "")

        submit_command = SUBMIT_COMMANDLINE_TEMPLATE.format(flow=flow_command, command=command)

        print("Executing:")
        print(submit_command)
        out = subprocess.check_output(submit_command.split(" "))
        print("\nCommand output")
        print("------")
        print(str(out, encoding='utf-8'))
