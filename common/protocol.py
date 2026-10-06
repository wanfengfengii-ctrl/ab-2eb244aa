"""Shared constants for the buoy snapshot protocol.

The buoy controller exposes a paged register read API.  Every successful page
carches the controller's current configuration revision.  A snapshot is valid
evidence only when *every* page was read under the same revision.
"""

# Controller paging limit (registers per page).
PAGE_SIZE = 64

# A single snapshot request may cover 1..512 registers.
MIN_REGISTERS = 1
MAX_REGISTERS = 512

# Modbus-style 16-bit register address space.  Start addresses must be positive
# and the whole window must fit in 65536 registers.
MIN_ADDRESS = 1
MAX_ADDRESS = 65536

# A complete read is retried from the first page at most this many times.
MAX_ATTEMPTS = 3

# HTTP transport settings (seconds).
PAGE_TIMEOUT = 3.0
GATEWAY_UPSTREAM_TIMEOUT = 15.0

# Terminal snapshot states.
STATE_COMPLETE = "complete"
STATE_CONFLICT = "conflict"


def validate_params(start_address, count):
    """Validate (start_address, register count).

    Returns an HTTP-style (status_code, message) tuple on failure, otherwise
    ``(None, None)``.
    """
    if not isinstance(start_address, int) or isinstance(start_address, bool):
        return 400, "startAddress must be an integer"
    if not isinstance(count, int) or isinstance(count, bool):
        return 400, "registerCount must be an integer"
    if start_address < MIN_ADDRESS:
        return 400, f"startAddress must be >= {MIN_ADDRESS}"
    if count < MIN_REGISTERS or count > MAX_REGISTERS:
        return 400, (
            f"registerCount must be between {MIN_REGISTERS} and "
            f"{MAX_REGISTERS}"
        )
    if start_address + count - 1 > MAX_ADDRESS:
        return 400, (
            f"register window exceeds address space "
            f"(last address {start_address + count - 1} > {MAX_ADDRESS})"
        )
    return None, None
