import pytest

from server import utils


def test_generate_lfdi_from_fingerprint():
    lfdi = utils._cert_fingerprint_to_lfdi(
        "0x3e4f45ab31edfe5b67e343e5e4562e31984e23e5349e2ad745672ed145ee213a"
    )  # fingerprint example from standard

    assert lfdi == "0x3e4f45ab31edfe5b67e343e5e4562e31984e23e5"
