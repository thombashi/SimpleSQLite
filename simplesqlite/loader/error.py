# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import


class ValidationError(Exception):
    """
    Raised data is not properly formatted.
    """


class InvalidDataError(Exception):
    """
    Raised when data is invalid to load.
    """


class OpenError(IOError):
    """
    Raised when failed to open a file.
    """
