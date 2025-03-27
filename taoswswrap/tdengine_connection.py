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

import multiprocessing
import os
import traceback
from threading import Semaphore, Thread
from typing import Any, Optional, Union

import taosws
from _queue import Empty
from taosws import TaosStmt


class QueryResult:
    def __init__(self, data, fields):
        self.data = data
        self.fields = fields

    def __eq__(self, other):
        return self.data == other.data and self.fields == other.fields

    def __repr__(self):
        return f"QueryResult({self.data}, {self.fields})"


class Field:
    def __init__(self, name, type, bytes):
        self.name = name
        self.type = type
        self.bytes = bytes

    def __eq__(self, other):
        return self.name == other.name and self.type == other.type and self.bytes == other.bytes

    def __repr__(self):
        return f"Field({self.name}, {self.type}, {self.bytes})"


class TDEngineError(Exception):
    pass


class ErrorResult:
    def __init__(self, tb, err):
        self.tb = tb
        self.err = err


def values_to_column(values: list, column_type: str):
    if column_type == "TIMESTAMP":
        timestamps = [round(timestamp.timestamp() * 1000) for timestamp in values]
        return taosws.millis_timestamps_to_column(timestamps)
    if column_type == "FLOAT":
        return taosws.floats_to_column(values)
    if column_type == "INT":
        return taosws.ints_to_column(values)
    if column_type.startswith("BINARY"):
        return taosws.binary_to_column(values)

    raise NotImplementedError(f"Unsupported column type '{column_type}'")


class Statement:
    def __init__(self, columns: dict[str, str], subtable: str, values: dict[str, Any]):
        self.columns = columns
        self.subtable = subtable
        self.values = values

    def prepare(self, statement: TaosStmt) -> TaosStmt:
        question_marks = ", ".join("?" * len(self.columns))
        statement.prepare(f"INSERT INTO ? VALUES ({question_marks});")
        statement.set_tbname(self.subtable)

        bind_params = []

        for col_name, col_type in self.columns.items():
            val = self.values[col_name]
            bind_params.append(values_to_column([val], col_type))

        statement.bind_param(bind_params)
        statement.add_batch()
        return statement


def _cleanup():
    # check for processes which have finished
    for p in list(multiprocessing.process._children):
        if (child_popen := p._popen) and child_popen.poll() is not None:
            multiprocessing.process._children.discard(p)


mp = multiprocessing.get_context("spawn")


# https://github.com/python/cpython/issues/104536
# Delete when support for python < 3.11.4 is dropped
class Process(mp.Process):
    def start(self):
        self._check_closed()
        assert self._popen is None, "cannot start a process twice"
        assert self._parent_pid == os.getpid(), "can only start a process object created by current process"
        assert not multiprocessing.process._current_process._config.get(
            "daemon"
        ), "daemonic processes are not allowed to have children"
        _cleanup()
        self._popen = self._Popen(self)
        self._sentinel = self._popen.sentinel
        # Avoid a refcycle if the target function holds an indirect
        # reference to the process object (see bpo-30775)
        del self._target, self._args, self._kwargs
        multiprocessing.process._children.add(self)

    def kill_and_close(self):
        self._popen.kill()
        self._popen.close()


def _run(connection_string, prefix_statements, q, statements, query):
    try:
        conn = taosws.connect(connection_string)

        for statement in prefix_statements + statements:
            if isinstance(statement, Statement):
                prepared_statement = statement.prepare(conn.statement())
                prepared_statement.execute()
            else:
                conn.execute(statement)

        if not query:
            q.put(None)
            return

        res = conn.query(query)

        # taosws.TaosField is not serializable
        fields = [Field(field.name(), field.type(), field.bytes()) for field in res.fields]

        q.put(QueryResult(list(res), fields))
    except Exception as e:
        tb = traceback.format_exc()
        q.put(ErrorResult(tb, e))


class TDEngineConnection:
    def __init__(self, connection_string, max_concurrency=16, run_directly=False):
        self._connection_string = connection_string
        self.prefix_statements = []
        self.semaphore = Semaphore(max_concurrency)

        if run_directly:
            self._conn = taosws.connect(self._connection_string)
            self.run = self.run_directly
        else:
            self.run = self.run_indirectly

    def run_indirectly(
        self,
        statements: Optional[Union[str, Statement, list[Union[str, Statement]]]] = None,
        query: Optional[str] = None,
        retries: int = 2,
        timeout: int = 5,
    ) -> Optional[QueryResult]:
        statements = statements or []
        if not isinstance(statements, list):
            statements = [statements]
        overall_retries = retries
        with self.semaphore:
            while retries >= 0:
                q = mp.Queue()
                process = Process(
                    target=_run, args=[self._connection_string, self.prefix_statements, q, statements, query]
                )
                try:
                    process.start()
                    try:
                        result = q.get(timeout=timeout)
                        if isinstance(result, ErrorResult):
                            if retries == 0:
                                query_msg_part = f" and query '{query}'" if query else ""
                                raise TDEngineError(
                                    f"TDEngine statements {statements}{query_msg_part} failed with an error after "
                                    f"{overall_retries} retries – {type(result.err).__name__}: {result.err}: "
                                    f"{result.tb}"
                                )
                        else:
                            return result
                    except Empty:
                        query_msg_part = f" and query '{query}'" if query else ""
                        if retries == 0:
                            raise TimeoutError(
                                f"TDEngine statements {statements}{query_msg_part} timed out after {timeout} seconds "
                                f"and {overall_retries} retries"
                            )
                    retries -= 1
                finally:
                    termination_thread = Thread(target=process.kill_and_close)
                    termination_thread.start()

    def run_directly(
        self,
        statements: Optional[Union[str, Statement, list[Union[str, Statement]]]] = None,
        query: Optional[str] = None,
        retries: int = 0,
        timeout: int = 0,
    ) -> Optional[QueryResult]:
        statements = statements or []
        if not isinstance(statements, list):
            statements = [statements]

        for statement in self.prefix_statements + statements:
            if isinstance(statement, Statement):
                prepared_statement = statement.prepare(self._conn.statement())
                prepared_statement.execute()
            else:
                self._conn.execute(statement)

        if not query:
            return

        res = self._conn.query(query)

        fields = [Field(field.name(), field.type(), field.bytes()) for field in res.fields]

        return QueryResult(list(res), fields)
