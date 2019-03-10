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
from threading import Lock

from atomicdict.dict_transaction import DictTransaction, DictTransactionError


class AtomicDict(object):
    """
    Allows wait-free-lock-free read (maximum possible performance) using atomic_waitfree_read().
    Writes are atomic using atomic_write_read().
    Also support transaction using begin_transaction() and commit_transaction().
    """

    def __init__(self, *args, **kwargs):
        """
        Initiate AtomicDict similarly to dict().

        :param args, kwargs: see dict()
        """

        # self.__immutable_store__ is immutable.
        # Once a a tuple (ver, dict) is assigned to this member, it should never be edited.
        # Therefore, any read operation on the object is inherently atomic, and thus wait-free.
        # A user should never access this member.
        self.__immutable_store__ = (0, dict(*args, **kwargs))
        self.__update_store_lock__ = Lock()

    ####################################################################
    # Atomic Ops
    ####################################################################

    def atomic_write_read(self, write_dict=None, read_keys=(),
                          remove_keys=(), default_value=None):
        """
        Updates the dict atomically.
        A corresponding read will see none of the modifications or all of them.

        :param write_dict: A dict contains the updates
        :param read_keys: A list of keys to read
        :param remove_keys: A list of keys to be removed
        :param default_value: A default value in case the key is not available
        :return: A dict of the values matching the requested keys
        """
        if write_dict is None:
            write_dict = {}
        with self.__update_store_lock__:
            ver, store = self.__immutable_store__
            read_values = {k: store.get(k, default_value) for k in read_keys}

            local_store = store.copy()
            local_store.update(write_dict)
            for k in remove_keys:
                if k in local_store:
                    del local_store[k]

            # Linearization point
            self.__immutable_store__ = (ver + 1, local_store)

            return read_values

    def atomic_waitfree_read(self, keys, default_value=None):
        """
        Read many keys atomically and promised to never wait.
        If a corresponding update will occur, the read will see none of the modifications or all of them.

        :param keys: A list of keys to read
        :param default_value: A default value in case the key is not available
        :return: A dict of the values matching the requested keys
        """

        # Linearization point
        ver, store = self.__immutable_store__

        return {k: store.get(k, default_value) for k in keys}

    ####################################################################
    # Transactions Ops
    ####################################################################

    def begin_transaction(self):
        """
        Init a transaction. Once done with the transaction, the user
        should try to commit this transaction.

        :return: A transaction object. Acts as a regular dict.
        """
        ver, store = self.__immutable_store__
        return DictTransaction(self, ver, store)

    def commit_transaction(self, transaction):
        """
        Commit a transaction object.
        If not successful, an DictTransactionException will be raised.
        Otherwise, the data will be updated in this dict.

        :param transaction: A transaction object obtained by
        :return: True if successful, raise exception otherwise
        """
        with self.__update_store_lock__:
            ver, store = self.__immutable_store__
            new_store = transaction.data
            transaction.validate_transaction(self)
            self.__immutable_store__ = (ver + 1, new_store)
            transaction.data = None

        return True

    def run_transaction(self, transaction_func, *args, **kwargs):
        """
        Run a transaction functions.

        :param transaction_func: A func object that accept a transaction object as its first parameter.
        :param args, kwargs: Arguments for the transaction function.
        :return: The transaction_func return value if successful.
        """
        success = False
        ret_value = None
        while not success:
            try:
                transaction = self.begin_transaction()
                ret_value = transaction_func(transaction, *args, **kwargs)
                transaction.commit()
            except DictTransactionError:
                success = False
            else:
                success = True
        return ret_value

    ####################################################################
    # Inherently atomic dict ops
    ####################################################################

    @property
    def ver(self):
        ver, store = self.__immutable_store__
        return ver

    def __len__(self):
        ver, store = self.__immutable_store__
        return len(store)

    def __repr__(self):
        ver, store = self.__immutable_store__
        return repr(store)

    def copy(self):
        ver, store = self.__immutable_store__
        return store.copy()

    def __getitem__(self, key):
        ver, store = self.__immutable_store__
        return store[key]

    def get(self, key, default_value=None):
        ver, store = self.__immutable_store__
        return store.get(key, default_value)

    def __contains__(self, item):
        ver, store = self.__immutable_store__
        return item in store

    def __iter__(self):
        ver, store = self.__immutable_store__
        return iter(store)

    def items(self):
        ver, store = self.__immutable_store__
        return store.items()

    def keys(self):
        ver, store = self.__immutable_store__
        return store.keys()

    def values(self):
        ver, store = self.__immutable_store__
        return store.values()

    ####################################################################
    # Non inherently atomic dict ops
    ####################################################################

    def clear(self):
        with self.__update_store_lock__:
            ver, store = self.__immutable_store__
            self.__immutable_store__ = (ver + 1, dict())

    def update(self, *args, **kwargs):
        self.atomic_write_read(write_dict=dict(*args, **kwargs))

    def __setitem__(self, key, item):
        self.atomic_write_read(write_dict={key: item})

    def __delitem__(self, key):
        self.atomic_write_read(remove_keys=(key,))

    def pop(self, key, default_value=None):
        res = self.atomic_write_read(read_keys=(key,), remove_keys=(key,),
                                     default_value=default_value)
        return res[key]
