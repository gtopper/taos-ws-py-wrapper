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

from setuptools import find_packages, setup


def load_deps(file_name):
    """Load dependencies from requirements file"""
    deps = []
    with open(file_name) as fp:
        for line in fp:
            line = line.strip()
            if not line or line[0] == "#":
                continue
            deps.append(line)
    return deps


install_requires = load_deps("requirements.txt")


with open("README.md") as fp:
    long_desc = fp.read()

setup(
    name="taos-ws-py-wrapper",
    version="0.1.0",
    description="Wrapper for TDEngine websocket client",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="Iguazio",
    author_email="galt@iguazio.com",
    license="Apache",
    url="https://github.com/gtopper/taos-ws-py-wrapper",
    packages=find_packages(include=["taos-ws-py-wrapper*"]),
    python_requires=">=3.9",
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
    ],
)
