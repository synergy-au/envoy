from fastapi import Depends, FastAPI

from server.api.depends import LFDIAuthDepends
from server.settings import AppSettings

settings = AppSettings()
lfdi_auth = LFDIAuthDepends(settings.cert_pem_header)


app = FastAPI(**settings.fastapi_kwargs, dependencies=[Depends(lfdi_auth)])
