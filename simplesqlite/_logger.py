# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import logbook


logger = logbook.Logger("SimpleSQLie")
logger.disable()


def set_logger(log_level):
    """
    Set logging level of this module. The module using
    `logbook <http://logbook.readthedocs.io/en/stable/>`__ module for logging.

    :param int log_level:
        One of the log level of the
        `logbook <http://logbook.readthedocs.io/en/stable/api/base.html>`__.
        Disabled logging if the ``log_level`` is ``logbook.NOTSET``.
    """

    if log_level == logbook.NOTSET:
        logger.disable()
    else:
        logger.enable()
        logger.level = log_level
