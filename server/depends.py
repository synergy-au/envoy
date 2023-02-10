from fastapi import Request, HTTPException

from server import utils
from server.crud import auth


class LFDIAuthDepends:
    def __init__(self, cert_pem_header: str):
        self.cert_pem_header = cert_pem_header

    async def __call__(
        self,
        request: Request,
    ) -> int:
        if self.cert_pem_header not in request.headers.keys():
            raise HTTPException(
                status_code=500, detail="Missing certificate PEM header from gateway."
            )  # Malformed

        cert_fingerprint = request.headers[self.cert_pem_header]

        # generate lfdi
        lfdi = utils.generate_lfdi_from_pem(cert_fingerprint)

        cert_id = await auth.select_certificateid_using_lfdi(lfdi)

        if not cert_id:
            raise HTTPException(
                status_code=403, detail="Unrecognised certificate ID."
            )  # Forbidden

        request.state.cert_id = cert_id
