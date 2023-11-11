"""Tests for iterator exercises"""
import unittest

from iterators import (
    first,
    is_iterator,
    Point,
    all_same,
    minmax,
    RandomNumberGenerator,
)


class FirstTests(unittest.TestCase):

    """Tests for first."""

    def test_iterator(self):
        self.assertEqual(first(iter([1, 2])), 1)

    def test_list(self):
        self.assertEqual(first([1, 2]), 1)


class IsIteratorTests(unittest.TestCase):

    """Tests for is_iterator."""

    def test_empty_iterator(self):
        self.assertTrue(is_iterator(iter([])))

    def test_list(self):
        self.assertFalse(is_iterator([1, 2]))

    def test_unmutated(self):
        i = iter([1, 2])
        self.assertTrue(is_iterator(i))
        self.assertEqual(list(i), [1, 2])

    def test_generator(self):
        def gen(): yield 4
        self.assertTrue(is_iterator(gen()))


class PointTests(unittest.TestCase):

    """Tests for Point."""

    def test_attributes(self):
        p = Point(1, 2, 3)
        self.assertEqual((p.x, p.y, p.z), (1, 2, 3))

    def test_multiple_assignment(self):
        x, y, z = Point(x=1, y=2, z=3)
        self.assertEqual((x, y, z), (1, 2, 3))


class AllSameTests(unittest.TestCase):

    """Tests for all_same."""

    def test_one_item_number(self):
        self.assertTrue(all_same([4]))
        self.assertTrue(all_same([0]))
        self.assertTrue(all_same([-1]))

    def test_one_string(self):
        self.assertTrue(all_same(['hello']))

    def test_one_none_value(self):
        self.assertTrue(all_same([None]))

    def test_one_tuple(self):
        self.assertTrue(all_same([()]))
        self.assertTrue(all_same([(1,)]))
        self.assertTrue(all_same([(1, 2)]))

    def test_empty_sequence(self):
        self.assertTrue(all_same([]))
        self.assertTrue(all_same(()))
        self.assertTrue(all_same(''))

    def test_two_same_item(self):
        self.assertTrue(all_same([1, 1]))
        self.assertTrue(all_same([0, 0]))
        self.assertTrue(all_same(['hello', 'hello']))
        self.assertTrue(all_same([-1, -1]))
        self.assertTrue(all_same([(1, 2), (1, 2)]))
        self.assertTrue(all_same([None, None]))

    def test_two_different_items(self):
        self.assertFalse(all_same(['hello', 'hi']))
        self.assertFalse(all_same([-1, 1]))
        self.assertFalse(all_same([-1, 'hi']))
        self.assertFalse(all_same([(1, 3), (1, 2)]))
        self.assertFalse(all_same(['hello', (4, 5)]))
        self.assertFalse(all_same([4, None]))
        self.assertFalse(all_same([None, 4]))

    def test_many_items(self):
        self.assertTrue(all_same([1, 1, 1, 1, 1, 1]))
        self.assertFalse(all_same([1, 1, 1, 1, 2, 1]))
        self.assertFalse(all_same(['hi', 'hello', 'hey']))
        self.assertFalse(all_same(['hello', 'hella', 'hello']))
        self.assertTrue(all_same(['hi', 'hi', 'hi', 'hi', 'hi']))
        self.assertTrue(all_same(['hello', 'hello', 'hello']))
        self.assertTrue(all_same([(1, 2, 3), (1, 2, 3), (1, 2, 3)]))
        self.assertFalse(all_same([(1, 2, 3), (1, 2, 3), (1, 4, 3)]))

    def test_nonhashable_values(self):
        self.assertFalse(all_same([['hi', 'hi'], ['hi', 'hi', 'hi']]))
        self.assertTrue(all_same([['hi', 'hi'], ['hi', 'hi']]))
        self.assertTrue(all_same([{1: 2}, {1: 2}]))
        self.assertFalse(all_same([{1: 2}, {1: 3}]))

    def test_nonsequences(self):
        numbers = [1, 3, 5, 7, 9]
        self.assertTrue(all_same({1}))
        self.assertFalse(all_same({1, 2}))
        self.assertFalse(all_same(n**2 for n in numbers))
        self.assertTrue(all_same(n % 2 for n in numbers))

    def test_return_early(self):
        self.assertFalse(all_same(n**2 for n in [2, 3, {}]))
        from itertools import count
        self.assertFalse(all_same(count()))


class MinMaxTests(unittest.TestCase):

    """Tests for minmax."""

    def assertIterableEqual(self, iterable1, iterable2):
        self.assertEqual(list(iterable1), list(iterable2))

    def test_ordered_numbers(self):
        self.assertIterableEqual(minmax([0, 1, 2, 3, 4]), (0, 4))

    def test_with_out_of_order_numbers(self):
        self.assertIterableEqual(minmax([10, 8, 7, 5.0, 3, 6, 2]), (2, 10))

    def test_single_item(self):
        self.assertIterableEqual(minmax([10]), (10, 10))

    def test_same_item_multiple_times(self):
        self.assertIterableEqual(minmax([8, 8, 8]), (8, 8))
        self.assertIterableEqual(minmax([7, 5, 6, 5, 7]), (5, 7))

    def test_negative_numbers(self):
        self.assertIterableEqual(minmax([-10, -8, -7, -5, -3]), (-10, -3))

    def test_strings(self):
        words = ["alfalfa", "animal", "apple", "acoustic", "axiom"]
        self.assertIterableEqual(minmax(words), ("acoustic", "axiom"))

    def test_mixed_types(self):
        with self.assertRaises(TypeError):
            minmax(['a', 2])

    def test_very_large_numbers(self):
        self.assertIterableEqual(
            minmax([2**1000, -2**1000]),
            (-2**1000, 2**1000),
        )

    def test_error_on_empty_iterable(self):
        with self.assertRaises(ValueError):
            minmax([])

    def test_with_non_lists(self):
        self.assertIterableEqual(minmax((89, 17, 70, 9)), (9, 89))
        self.assertIterableEqual(minmax({8, 7, 5, 3, 9, 6, 2}), (2, 9))
        self.assertIterableEqual(minmax(n**2 for n in range(1, 4)), (1, 9))
        with self.assertRaises(ValueError):
            minmax(iter([]))


class RandomNumberGeneratorTests(unittest.TestCase):

    """Tests for RandomNumberGenerator."""

    def test_zero_through_five(self):
        number_generator = RandomNumberGenerator(0, 5)
        numbers = set()
        for i, n in enumerate(number_generator):
            numbers.add(n)
            if i > 100:
                break
        self.assertEqual(numbers, {0, 1, 2, 3, 4, 5})

    def test_fifty_through_seventy(self):
        number_generator = RandomNumberGenerator(50, 70)
        numbers = set()
        for i, n in enumerate(number_generator):
            numbers.add(n)
            if i > 500:
                break
        self.assertEqual(numbers, set(range(50, 71)))

    def test_actually_random(self):
        generator1 = RandomNumberGenerator(0, 1000)
        generator2 = RandomNumberGenerator(0, 1000)
        for i, (n, m) in enumerate(zip(generator1, generator2)):
            if n != m:
                break
            if i > 500:
                self.fail('500 non-random numbers found')

    def test_is_iterator(self):
        number_generator = RandomNumberGenerator(0, 100)
        self.assertIs(iter(number_generator), number_generator)


if __name__ == "__main__":
    unittest.main()
