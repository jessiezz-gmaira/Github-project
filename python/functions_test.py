"""Test for generator function exercises"""
from collections.abc import Generator
import sys
import unittest

from functions import (
    unique,
    float_range,
    head,
    pairwise,
    around,
    stop_on,
    deep_flatten,
    get_primes_over,
)


class UniqueTests(unittest.TestCase):

    """Tests for unique."""

    def test_list_with_duplicates(self):
        inputs = [6, 7, 0, 9, 0, 1, 2, 7, 7, 9]
        outputs = [6, 7, 0, 9, 1, 2]
        self.assertEqual(list(unique(inputs)), outputs)

    def test_empty_list(self):
        self.assertEqual(list(unique([])), [])

    def test_string(self):
        self.assertEqual(''.join(unique("hello there")), 'helo tr')


class FloatRangeTests(unittest.TestCase):

    """Tests for float_range."""

    def test_has_iterability(self):
        self.assertEqual(list(float_range(1, 11, 2)), [1, 3, 5, 7, 9])
        self.assertEqual(
            list(float_range(0.5, 7, 0.75)),
            [0.5, 1.25, 2.0, 2.75, 3.5, 4.25, 5.0, 5.75, 6.5]
        )

    def test_optional_step(self):
        self.assertEqual(list(float_range(1, 6, 1)), [1, 2, 3, 4, 5])
        self.assertEqual(list(float_range(1, 6)), [1, 2, 3, 4, 5])
        self.assertEqual(
            list(float_range(0.5, 6)),
            [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]
        )

    def test_fractional_step_size(self):
        self.assertEqual(
            list(float_range(1, 6, 0.5)),
            [1, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5]
        )
        self.assertEqual(
            list(float_range(1, 5.6, 0.5)),
            [1, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5]
        )

    def test_no_arguments(self):
        with self.assertRaises(TypeError):
            float_range()

    def test_too_many_arguments(self):
        with self.assertRaises(TypeError):
            float_range(0, 5, 1, 1)
        with self.assertRaises(TypeError):
            float_range(0, 5, 1, 1, 1)

    def test_no_memory_used(self):
        """Make sure float_range response isn't a giant list of numbers."""
        response = float_range(0, 1024, 2**-4)
        if isinstance(response, Generator):
            next(response)
            size = sum(
                sys.getsizeof(obj)
                for obj in response.gi_frame.f_locals.values()
            )
        else:
            size = sys.getsizeof(response)
        self.assertLess(size, 8000, 'Too much memory used')
        self.assertNotEqual(type(response), list)
        self.assertNotEqual(type(response), tuple)

    # Comment this line to test the length of float_range objects
    @unittest.skip("length bonus")
    def test_has_length(self):
        self.assertEqual(len(float_range(100)), 100)
        self.assertEqual(len(float_range(1, 100)), 99)
        self.assertEqual(len(float_range(1, 11, 2)), 5)
        self.assertEqual(len(float_range(0.5, 7, 0.75)), 9)
        self.assertEqual(len(float_range(1000000)), 1000000)
        self.assertEqual(len(float_range(11, 1.2, -2)), 5)
        self.assertEqual(len(float_range(11, 1.2, 2)), 0)


class HeadTests(unittest.TestCase):

    """Tests for head."""

    def test_first_two(self):
        self.assertEqual(list(head([1, 2, 3, 4, 5], n=2)), [1, 2])

    def test_iterator(self):
        output = head([1, 2, 3, 4, 5], n=4)
        self.assertEqual(list(zip(output, output)), [(1, 2), (3, 4)])

    def test_more_items_than_exist(self):
        self.assertEqual(list(head([1], n=2)), [1])


class PairwiseTests(unittest.TestCase):

    """Tests for pairwise."""

    def test_three(self):
        inputs = [1, 2, 3]
        outputs = [(1, 2), (2, 3), (3, None)]
        self.assertEqual(list(pairwise(inputs)), outputs)

    def test_empty(self):
        self.assertEqual(list(pairwise([])), [])

    def test_one_item(self):
        self.assertEqual(list(pairwise([1])), [(1, None)])

    @unittest.skip
    def test_none(self):
        inputs = [None, None]
        outputs = [(None, None), (None, None)]
        self.assertEqual(list(pairwise(inputs)), outputs)

    def test_string(self):
        inputs = "hey"
        outputs = [('h', 'e'), ('e', 'y'), ('y', None)]
        self.assertEqual(list(pairwise(inputs)), outputs)


class AroundTests(unittest.TestCase):

    """Tests for around."""

    def test_four(self):
        inputs = [1, 2, 3, 4]
        outputs = [(None, 1, 2), (1, 2, 3), (2, 3, 4), (3, 4, None)]
        self.assertEqual(list(around(inputs)), outputs)

    def test_empty(self):
        self.assertEqual(list(around([])), [])

    def test_one_item(self):
        self.assertEqual(list(around([1])), [(None, 1, None)])

    @unittest.skip
    def test_none(self):
        inputs = [None, None]
        outputs = [(None, None, None), (None, None, None)]
        self.assertEqual(list(around(inputs)), outputs)

    def test_string(self):
        inputs = "hey"
        outputs = [(None, 'h', 'e'), ('h', 'e', 'y'), ('e', 'y', None)]
        self.assertEqual(list(around(inputs)), outputs)


class StopOnTests(unittest.TestCase):

    """Tests for stop_on."""

    def test_last_item(self):
        self.assertEqual(list(stop_on([1, 2, 3], 3)), [1, 2])

    def test_first_item(self):
        self.assertEqual(list(stop_on([1, 2, 3], 1)), [])

    def test_not_in(self):
        self.assertEqual(list(stop_on([1, 2, 3], 4)), [1, 2, 3])

    def test_repeats(self):
        self.assertEqual(list(stop_on([1, 1, 2, 2, 1, 2], 2)), [1, 1])


class DeepFlattenTests(unittest.TestCase):

    """Tests for deep_flatten."""

    def test_deep_lists(self):
        inputs = [0, [1, [2, 3]], [4]]
        outputs = [0, 1, 2, 3, 4]
        self.assertEqual(list(deep_flatten(inputs)), outputs)

    def test_empty_deep_list(self):
        self.assertEqual(list(deep_flatten([[()]])), [])


class GetPrimesOverTests(unittest.TestCase):

    """Tests for get_primes_over."""

    def test_10_primes_over_one_million(self):
        ten_primes = [1000003, 1000033, 1000037, 1000039, 1000081,
                      1000099, 1000117, 1000121, 1000133, 1000151]
        self.assertSequenceEqual(list(get_primes_over(10)), ten_primes)

    def test_first_prime_over_one_million(self):
        self.assertSequenceEqual(list(get_primes_over(1)), [1000003])


if __name__ == "__main__":
    unittest.main()
