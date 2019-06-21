#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from py_prog_load.skeleton import fib

__author__ = "David Longo"
__copyright__ = "David Longo"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
