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

import taosws
from _queue import Empty


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


class Statement:
    def __init__(self, function, kwargs):
        self.function = function
        self.kwargs = kwargs

    def prepare(self, statement):
        return self.function(statement, **self.kwargs)


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


class TDEngineConnection:
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self.prefix_statements = []

    def _run(self, q, statements, query):
        try:
            conn = taosws.connect(self._connection_string)

            for statement in self.prefix_statements + statements:
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

    def run(self, statements=None, query=None, retries=2, timeout=5):
        statements = statements or []
        if not isinstance(statements, list):
            statements = [statements]
        overall_retries = retries
        while retries >= 0:
            q = mp.Queue()
            process = Process(target=self._run, args=[q, statements, query])
            try:
                process.start()
                process.join(timeout=timeout)
                try:
                    result = q.get(timeout=0)
                    if isinstance(result, ErrorResult):
                        if retries == 0:
                            query_msg_part = f" and query '{query}'" if query else ""
                            raise TDEngineError(
                                f"TDEngine statements {statements}{query_msg_part} failed with an error after "
                                f"{overall_retries} retries – {type(result.err).__name__}: {result.err}: {result.tb}"
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
                try:
                    process.kill()
                    process.close()
                except Exception:
                    pass
