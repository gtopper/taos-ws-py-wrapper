# Copyright 2024 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from datetime import datetime, timezone

import pytest

from taoswswrap.tdengine_connection import Field, TDEngineConnection, TDEngineError

connection_string = os.getenv("TAOSWS_CONNECTION_STRING")


def is_tdengine_defined() -> bool:
    return connection_string is not None and connection_string.startswith("taosws://")


@pytest.mark.skipif(not is_tdengine_defined(), reason="TDEngine is not defined")
def test_tdengine_connection():
    conn = TDEngineConnection(connection_string)

    res = conn.run(
        statements=[
            "DROP DATABASE IF EXISTS mydb",
            "CREATE DATABASE mydb",
            "USE mydb",
            "CREATE STABLE mystable (column1 TIMESTAMP, column2 FLOAT) TAGS (tag1 INT);",
            "CREATE TABLE mytable USING mystable TAGS (1)",
            "INSERT INTO mytable VALUES (1728444786455, 1)",
        ],
        query="SELECT * FROM mytable",
    )
    assert res.fields == [Field("column1", "TIMESTAMP", 8), Field("column2", "FLOAT", 4)]
    assert len(res.data) == 1
    data = res.data[0]
    assert len(data) == 2
    col1, col2 = data
    assert datetime.strptime(col1, "%Y-%m-%d %H:%M:%S.%f %z").astimezone(timezone.utc) == datetime(
        2024, 10, 9, 3, 33, 6, 455000, tzinfo=timezone.utc
    )
    assert col2 == 1


@pytest.mark.skipif(not is_tdengine_defined(), reason="TDEngine is not defined")
def test_tdengine_connection_error_propagation():
    conn = TDEngineConnection(connection_string)

    try:
        conn.run(statements="USE idontexist")
        pytest.fail("Expected an error")
    except TDEngineError as e:
        assert "TDEngine statements ['USE idontexist'] failed with an error after 2 retries" in str(e)
        assert "Internal error: `Database not exist`" in str(e)


@pytest.mark.skipif(not is_tdengine_defined(), reason="TDEngine is not defined")
def test_tdengine_connection_create_db():
    conn = TDEngineConnection(connection_string)

    res = conn.run(statements="CREATE DATABASE IF NOT EXISTS mydb")
    assert res is None
