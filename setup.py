"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
from setuptools import setup

setup(
    name="atomicdict",
    version="0.1.0",
    py_modules=['atomicdict'],
    description="Wait-free-lock-free read/write/remove (maximum possible performance) with optional transaction mode.",
    author="Liran Funaro",
    author_email="liran.funaro+atomicdict@gmail.com",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/liran-funaro/atomicdict",
    keywords='atomic dict lock-free wait-free transaction',
    license='GPL',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
