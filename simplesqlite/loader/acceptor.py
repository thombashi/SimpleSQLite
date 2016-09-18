# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
import abc

import six


@six.add_metaclass(abc.ABCMeta)
class LoaderAcceptorInterface(object):
    """
    Interface class of table loader acceptor.
    """

    @abc.abstractmethod
    def accept(self, loader):  # pragma: no cover
        pass


class LoaderAcceptor(LoaderAcceptorInterface):
    """
    Abstract class of table loader acceptor.
    """

    def __init__(self):
        self._loader = None

    def accept(self, loader):
        self._loader = loader
