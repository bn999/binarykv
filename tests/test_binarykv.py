import unittest
import os
import sys
from pathlib import Path

# Add the parent directory to the sys.path for imports if the package isn't installed
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from binarykv.binarykv import BinaryKV  # Import the BinaryKV class


class TestBinaryKV(unittest.TestCase):
    def setUp(self):
        """Set up a temporary BinaryKV instance and sample data."""
        self.filename = 'test_data'
        self.kv_store = BinaryKV(self.filename)

        self.records = {
            f"key_{i}".encode(): f"Record_{i}".encode()
            for i in range(16)
        }

    def tearDown(self):
        """Clean up test files."""
        if os.path.exists(self.filename + '.bin'):
            os.remove(self.filename + '.bin')
        if os.path.exists(self.filename + '.idx'):
            os.remove(self.filename + '.idx')

    def test_write_and_read(self):
        """Test writing and reading records."""
        self.kv_store.open(mode='w')
        for key, record in self.records.items():
            self.kv_store.write(key, record)
        self.kv_store.close()

        # Reopen in read mode
        self.kv_store.open(mode='r')
        for key, offset in self.kv_store.scan_index():
            value = self.kv_store.read(offset)
            self.assertIn(key, self.records)
            self.assertEqual(value, self.records[key])

        self.kv_store.close()

    def test_read_by_offset(self):
        """Test reading a record by offset."""
        self.kv_store.open(mode='w')
        for key, record in self.records.items():
            self.kv_store.write(key, record)
        self.kv_store.close()

        # Reopen in read mode
        self.kv_store.open(mode='r')
        offset = next(self.kv_store.scan_index())[1]  # Get the offset of the first record
        value = self.kv_store.read(offset)
        expected_key = b'key_0'
        self.assertEqual(value, self.records[expected_key])

        self.kv_store.close()


if __name__ == '__main__':
    unittest.main()
