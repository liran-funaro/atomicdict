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
from collections import UserDict


class DictTransactionError(Exception):
    def __init__(self, source, version, msg=None):
        self.source = source
        self.version = version
        Exception.__init__(self, msg)


class DictTransaction(UserDict):

    def __init__(self, source_atomic_dict, source_ver, source_store):
        """
        Should be created only by AtomicDict.begin_transaction().

        :param source_atomic_dict: The AtomicDict that created this transaction.
        :param source_ver: The version of the store.
        :param source_store: The store (will be copied).
        """
        UserDict.__init__(source_store)
        self.__source__ = source_atomic_dict
        self.__ver__ = source_ver
        self.__committed__ = False

    @property
    def ver(self):
        return self.__ver__

    @property
    def source(self):
        return self.__source__

    @property
    def is_committed(self):
        return self.__committed__

    def validate_transaction(self, atomic_dict):
        """
        Validate the transaction.

        :param atomic_dict: The dict that should be committed.
        :return: True if successful, raise exception otherwise.
        """
        if atomic_dict is not self.__source__:
            raise DictTransactionError(self.__source__, self.__ver__,
                                       "Transaction object does not match input object")

        cur_ver = atomic_dict.ver
        if cur_ver != self.__ver__:
            raise DictTransactionError(self.__source__, self.__ver__,
                                       f"Expected version: {self.__ver__} - Actual version: {cur_ver}")
        return True

    def commit(self):
        if not self.__committed__:
            self.__committed__ = self.__source__.commit_transaction(self)

    def abort(self, reason=None):
        if reason:
            msg = f"Transaction aborted by user: {reason}"
        else:
            msg = "Transaction aborted by user."
        raise DictTransactionError(self.__source__, self.__ver__, msg)

    ########################################################################################################
    # With Statement Context Manager
    ########################################################################################################

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
