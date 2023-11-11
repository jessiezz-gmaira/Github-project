"""Test for itertools exercises"""
import unittest

from iteration import total_length, lstrip


class TotalLengthTests(unittest.TestCase):

    """Tests for total_length."""

    def test_list(self):
        self.assertEqual(total_length([1, 2, 3]), 3)

    def test_nothing(self):
        self.assertEqual(total_length(), 0)

    def test_iterators(self):
        self.assertEqual(total_length([1, 2, 3], [4, 5], iter([6, 7])), 7)


class LStripTests(unittest.TestCase):

    """Tests for lstrip."""

    def assertIterableEqual(self, iterable1, iterable2):
        self.assertEqual(list(iterable1), list(iterable2))

    def test_list(self):
        self.assertIterableEqual(lstrip([1, 1, 2, 3, 1], 1), [2, 3, 1])

    def test_nothing_to_strip(self):
        self.assertIterableEqual(lstrip([1, 2, 3], 0), [1, 2, 3])

    def test_string(self):
        self.assertIterableEqual(lstrip('  hello', ' '), 'hello')

    def test_empty_iterable(self):
        self.assertIterableEqual(lstrip([], 1), [])

    def test_strip_all(self):
        self.assertIterableEqual(lstrip([1, 1, 1], 1), [])

    def test_none_values(self):
        self.assertIterableEqual(lstrip([None, 1, 2, 3], 0), [None, 1, 2, 3])

    def test_iterator(self):
        squares = (n**2 for n in [0, 0, 1, 2, 3])
        self.assertIterableEqual(lstrip(squares, 0), [1, 4, 9])

    def test_returns_iterator(self):
        stripped = lstrip((1, 2, 3), 1)
        self.assertEqual(iter(stripped), iter(stripped))


if __name__ == "__main__":
    unittest.main()
