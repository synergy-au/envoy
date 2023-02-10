import pytest

from fastapi import Request, HTTPException

from server.depends import LFDIAuthDepends
from server.main import settings
from .resources import TEST_CERTIFICATE_PEM


bs_cert_pem_header = bytes(f"{settings.cert_pem_header}", "ascii")


@pytest.mark.asyncio
async def test_lfdiauthdepends_request_with_no_certpemheader_expect_500_response():
    req = Request({"type": "http", "headers": {}})

    lfdi_dep = LFDIAuthDepends(settings.cert_pem_header)

    with pytest.raises(HTTPException) as exc:
        await lfdi_dep(req)

    assert exc.value.status_code == 500


@pytest.mark.asyncio
async def test_lfdiauthdepends_request_with_unregistered_cert_expect_403_response(
    mocker,
):
    mocker.patch("server.crud.auth.select_certificateid_using_lfdi", return_value=None)
    req = Request(
        {
            "type": "http",
            "headers": [
                (
                    bs_cert_pem_header,
                    TEST_CERTIFICATE_PEM,
                )
            ],
        }
    )

    lfdi_dep = LFDIAuthDepends(settings.cert_pem_header)

    with pytest.raises(HTTPException) as exc:
        await lfdi_dep(req)

    assert exc.value.status_code == 403
