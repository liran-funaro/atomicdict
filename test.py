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
from random import sample
from threading import Event, Thread
from unittest import TestCase

from atomicdict import AtomicDict


def update_forever(d, e, update_set):
    local = d.atomic_waitfree_read(update_set)
    while not e.is_set():
        for k in update_set:
            local[k] += 1
        d.atomic_write_read(write_dict=local)


class TestAtomicDict(TestCase):
    def test_write_vs_read(self):
        d = AtomicDict({b: 0 for b in range(10000)})
        update_set = sample(d.keys(), 20)
        e = Event()
        Thread(target=update_forever, args=(d, e, update_set)).start()
        try:
            for _ in range(100000):
                res = d.atomic_waitfree_read(keys=update_set)
                val = res[update_set[0]]
                all_ok = all(res[k] == val for k in update_set)
                self.assertTrue(all_ok)
        finally:
            e.set()
