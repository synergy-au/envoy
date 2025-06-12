from typing import Iterable

from datetime import datetime

from envoy_schema.admin import schema

from envoy.server import model


class CertificateMapper:
    @staticmethod
    def map_to_response(certificate: model.Certificate) -> schema.CertificateResponse:
        """Converts an internal Certificate model to the schema CertificateResponse"""

        return schema.CertificateResponse(
            certificate_id=certificate.certificate_id,
            created=certificate.created,
            lfdi=certificate.lfdi,
            expiry=certificate.expiry,
        )

    @staticmethod
    def map_to_page_response(
        total_count: int, start: int, limit: int, certificates: Iterable[model.Certificate]
    ) -> schema.CertificatePageResponse:
        """Converts a page of Certificate models to the schema CertificatePageResponse"""
        return schema.CertificatePageResponse(
            total_count=total_count,
            start=start,
            limit=limit,
            certificates=[CertificateMapper.map_to_response(a) for a in certificates],
        )


class CertificateListMapper:
    @staticmethod
    def map_from_request(
        created: datetime, certificate_list: list[schema.CertificateRequest]
    ) -> list[model.Certificate]:
        """Converts a list of CertificateRequest objects into model objects"""
        return [
            model.Certificate(
                created=created,
                lfdi=cr.lfdi,
                expiry=cr.expiry,
            ) for cr in certificate_list
        ]
