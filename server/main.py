from fastapi import FastAPI, Depends

from server.settings import AppSettings
from server.depends import LFDIAuthDepends


settings = AppSettings()
lfdi_auth = LFDIAuthDepends(settings.cert_pem_header)


app = FastAPI(**settings.fastapi_kwargs, dependencies=[Depends(lfdi_auth)])
