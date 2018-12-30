# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.flow -- TODO
===================================

.. module:: flow
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
from ._version import get_versions
from .resources import FlowResources

VERSIONS = get_versions()
del get_versions

__descr__ = 'TODO'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018, Xavier Bouthillier'
__url__ = 'https://github.com/bouthilx/mahler.scheduler.flow'


def build(max_workers, **kwargs):
    return FlowResources(max_workers=max_workers)


def build_parser(parser):
    """Return the parser that needs to be used for this command"""
    flow_parser = parser.add_parser('flow', help='flow help')

    flow_parser.add_argument(
        '--max-workers', type=int,
        help='number of concurrent workers to submit.')
