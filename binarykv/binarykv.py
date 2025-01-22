import struct

class BinaryKV:
    def __init__(self, filename, mode=None):
        """
        Initialize the BinaryKV store with an optional mode.
        If a mode is provided, it automatically opens the files in that mode.
        """
        self.data_filename = filename + '.bin'
        self.index_filename = filename + '.idx'
        self.data_file = None
        self.index_file = None

        # If a mode is provided, open the files
        if mode is not None:
            self.open(mode)

    def open(self, mode='r'):
        """Open the data and index files in the specified mode ('r' for read, 'w' for write, 'a' for append)."""
        if mode == 'w':
            # Overwrite mode: create or truncate the files
            self.data_file = open(self.data_filename, 'wb')  # Write mode, truncate the file
            self.index_file = open(self.index_filename, 'wb')
        elif mode == 'a':
            # Append mode: open the files for appending
            self.data_file = open(self.data_filename, 'ab')  # Append mode
            self.index_file = open(self.index_filename, 'ab')
        elif mode == 'r':
            # Read mode: open the files for reading
            self.data_file = open(self.data_filename, 'rb')
            self.index_file = open(self.index_filename, 'rb')
        else:
            raise ValueError("Mode must be 'r' for reading, 'w' for writing, or 'a' for appending")

    def write(self, key: bytes, record: bytes):
        """Write a binary key and binary record to the data file and store its position in the index."""
        if not self.data_file or not self.index_file:
            raise IOError("Files are not opened in write mode")

        # Store the current position (offset) in the data file
        pos = self.data_file.tell()

        key_length = 0 if key is None else len(key)

        # Write the key length, key (if present), and offset to the index file
        self.index_file.write(struct.pack('I', key_length))  # Key length (4 bytes)
        if key is not None:
            self.index_file.write(key)  # Key data (if present)
        self.index_file.write(struct.pack('Q', pos))  # Offset in the data file (8 bytes)

        # Write the record length and the actual data to the data file
        self.data_file.write(struct.pack('I', len(record)))  # Record length (4 bytes)
        self.data_file.write(record)  # Actual record data

    def read(self, offset: int) -> bytes:
        """Read a binary record by its offset."""
        if not self.data_file:
            raise IOError("Data file is not opened in read mode")

        self.data_file.seek(offset)
        record_length = struct.unpack('I', self.data_file.read(4))[0]
        return self.data_file.read(record_length)

    def scan_index(self):
        """Scan the index and yield (key, offset) tuples. Return None for keys that had a length of 0."""
        if not self.index_file:
            raise IOError("Files are not opened in read mode")

        # Seek to the beginning of the index file
        self.index_file.seek(0)

        while True:
            # Read the key length (4 bytes)
            key_len_bytes = self.index_file.read(4)
            if not key_len_bytes:
                break  # End of index file

            key_len = struct.unpack('I', key_len_bytes)[0]
            key = None if key_len == 0 else self.index_file.read(key_len)

            # Read the offset (8 bytes)
            offset = struct.unpack('Q', self.index_file.read(8))[0]

            yield key, offset

    def scan_data(self):
        """Scan the data file and yield (offset, record) tuples."""
        if not self.data_file:
            raise IOError("Data file is not opened in read mode")

        self.data_file.seek(0)

        while True:
            offset = self.data_file.tell()

            # Read the record length (4 bytes)
            record_len_bytes = self.data_file.read(4)
            if not record_len_bytes:
                break  # End of data file

            record_len = struct.unpack('I', record_len_bytes)[0]
            record = self.data_file.read(record_len)

            yield offset, record

    def close(self):
        """Close the data and index files."""
        if self.data_file:
            self.data_file.close()
        if self.index_file:
            self.index_file.close()
