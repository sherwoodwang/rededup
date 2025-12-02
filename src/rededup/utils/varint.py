"""Variable-length integer encoding utilities.

This module provides UTF-8 style variable-length encoding for integers,
supporting values from 0 to 2^63-1 (63-bit integers).
"""


def encode_varint(value: int) -> bytes:
    """Encode an integer as variable-length bytes (UTF-8 style).

    Supports values from 0 to 2^63-1 (63-bit integers).

    Encoding format:
    - 0 to 127 (2^7-1): 1 byte - 0xxxxxxx
    - 128 to 16383 (2^14-1): 2 bytes - 10xxxxxx xxxxxxxx
    - 16384 to 2097151 (2^21-1): 3 bytes - 110xxxxx xxxxxxxx xxxxxxxx
    - ... up to 9 bytes for 63-bit values

    Args:
        value: Non-negative integer to encode (max 2^63-1)

    Returns:
        Variable-length byte encoding

    Raises:
        ValueError: If value is negative or exceeds 2^63-1
    """
    if value < 0:
        raise ValueError(f"Cannot encode negative value: {value}")
    if value >= (1 << 63):
        raise ValueError(f"Value {value} exceeds maximum (2^63-1)")

    # Determine byte count needed
    if value < (1 << 7):
        byte_count = 1
    elif value < (1 << 14):
        byte_count = 2
    elif value < (1 << 21):
        byte_count = 3
    elif value < (1 << 28):
        byte_count = 4
    elif value < (1 << 35):
        byte_count = 5
    elif value < (1 << 42):
        byte_count = 6
    elif value < (1 << 49):
        byte_count = 7
    elif value < (1 << 56):
        byte_count = 8
    else:
        byte_count = 9

    # Build prefix bits
    if byte_count == 1:
        prefix = 0
    else:
        prefix = ((1 << byte_count) - 1) << (8 - byte_count)

    # Calculate payload bits
    payload_bits = byte_count * 8 - byte_count
    payload_mask = (1 << payload_bits) - 1
    payload = value & payload_mask

    # Combine prefix with high bits of payload
    high_byte = prefix | (payload >> ((byte_count - 1) * 8))

    # Build result bytes
    result = bytearray([high_byte])
    for i in range(byte_count - 2, -1, -1):
        result.append((payload >> (i * 8)) & 0xFF)

    return bytes(result)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a variable-length integer from bytes.

    Args:
        data: Byte array containing the varint
        offset: Starting position in the byte array

    Returns:
        Tuple of (decoded_value, bytes_consumed)

    Raises:
        ValueError: If data is invalid or insufficient
    """
    if offset >= len(data):
        raise ValueError("Offset exceeds data length")

    first_byte = data[offset]

    # Count leading 1 bits to determine byte count
    byte_count = 0
    mask = 0x80
    while byte_count < 8 and (first_byte & mask):
        byte_count += 1
        mask >>= 1

    if byte_count == 0:
        byte_count = 1
    elif byte_count == 8:
        byte_count = 9

    if offset + byte_count > len(data):
        raise ValueError(f"Insufficient data: need {byte_count} bytes, have {len(data) - offset}")

    # Extract payload bits
    if byte_count == 1:
        high_bits = first_byte
    else:
        high_mask = (1 << (8 - byte_count)) - 1
        high_bits = first_byte & high_mask

    value = high_bits
    for i in range(1, byte_count):
        value = (value << 8) | data[offset + i]

    return value, byte_count
