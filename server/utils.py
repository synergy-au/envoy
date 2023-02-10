""" For utility functions that have no where else to logically exist.
"""
import base64
import urllib.parse
import hashlib


def generate_lfdi_from_pem(cert_pem: str) -> str:
    """This function generates the 2030.5-2018 lFDI (Long-form device identifier) from the device's
    TLS certificate in pem (Privacy Enhanced Mail) format, i.e. Base64 encoded DER
    (Distinguished Encoding Rules) certificate, as decribed in Section 6.3.4
    of IEEE Std 2030.5-2018.

    The lFDI is derived, from the certificate in PEM format, according to the following steps:
        1- Base64 decode the PEM to DER.
        2- Performing SHA256 hash on the DER to generate the certificate fingerprint.
        3- Left truncating the certificate fingerprint to 160 bits.

    Args:
        cert_pem: TLS certificate in PEM format.

    Return:
        The lFDI as a hex string.
    """
    # generate lfdi
    return _cert_fingerprint_to_lfdi(_cert_pem_to_cert_fingerprint(cert_pem))


def _cert_fingerprint_to_lfdi(cert_fingerprint: str) -> str:
    return cert_fingerprint[:42]


def _cert_pem_to_cert_fingerprint(cert_pem: str) -> str:
    # URL/percent decode
    cert_pem = urllib.parse.unquote(cert_pem)

    # remove header/footer
    cert_pem = "\n".join(cert_pem.splitlines()[1:-1])

    # decode base64
    cert_pem = base64.b64decode(cert_pem)

    # sha256 hash
    hashing_obj = hashlib.sha256(cert_pem)
    return hashing_obj.hexdigest()
