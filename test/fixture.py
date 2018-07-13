# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, print_function, unicode_literals

import pytest
from simplesqlite import SimpleSQLite


TEST_TABLE_NAME = "test_table"


@pytest.fixture
def con(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_from_data_matrix(
        table_name=TEST_TABLE_NAME,
        attr_name_list=["attr_a", "attr_b"],
        data_matrix=[[1, 2], [3, 4]],
    )

    return con


@pytest.fixture
def con_mix(tmpdir):
    p = tmpdir.join("tmp_mixed_data.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_from_data_matrix(
        table_name=TEST_TABLE_NAME,
        attr_name_list=["attr_i", "attr_f", "attr_s"],
        data_matrix=[[1, 2.2, "aa"], [3, 4.4, "bb"]],
    )

    return con


@pytest.fixture
def con_ro(tmpdir):
    p = tmpdir.join("tmp_readonly.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_from_data_matrix(
        table_name=TEST_TABLE_NAME,
        attr_name_list=["attr_a", "attr_b"],
        data_matrix=[[1, 2], [3, 4]],
    )
    con.close()
    con.connect(str(p), "r")

    return con


@pytest.fixture
def con_profile(tmpdir):
    p = tmpdir.join("tmp_profile.db")
    con = SimpleSQLite(str(p), "w", profile=True)

    con.create_table_from_data_matrix(
        table_name=TEST_TABLE_NAME,
        attr_name_list=["attr_a", "attr_b"],
        data_matrix=[[1, 2], [3, 4]],
    )
    con.commit()

    return con


@pytest.fixture
def con_index(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_from_data_matrix(
        table_name=TEST_TABLE_NAME,
        attr_name_list=["attr_a", "attr_b"],
        data_matrix=[[1, 2], [3, 4]],
        index_attr_list=["attr_a"],
    )

    return con


@pytest.fixture
def con_null(tmpdir):
    p = tmpdir.join("tmp_null.db")
    con = SimpleSQLite(str(p), "w")
    con.close()

    return con


@pytest.fixture
def con_empty(tmpdir):
    p = tmpdir.join("tmp_empty.db")
    return SimpleSQLite(str(p), "w")
