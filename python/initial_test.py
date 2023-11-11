"""Test to confirm that the test framework is working."""
import unittest
from initial import is_ok


class InitialTests(unittest.TestCase):
    """Tests for is_ok."""

    def test_confirm_test(self):
        """Test passed."""
        confirm = is_ok()
        print(confirm)


if __name__ == '__main__':
    unittest.main()
