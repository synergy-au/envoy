from sqlalchemy.ext.asyncio import AsyncSession
from envoy_schema.admin import schema

from envoy.admin import crud
from envoy.server import crud as server_crud
from envoy.admin import mapper


class CertificateManager:
    @staticmethod
    async def fetch_many_certificates_for_aggregator(
        session: AsyncSession, aggregator_id: int, start: int, limit: int
    ) -> schema.CertificatePageResponse | None:
        """Select many certificates from the DB and map to a list of CertificateResponse objects"""
        # Assess aggregator exists
        if not await server_crud.aggregator.select_aggregator(session, aggregator_id):
            return None
        cert_list = await crud.certificate.select_all_certificates_for_aggregator(session, aggregator_id, start, limit)
        cert_count = await crud.certificate.count_certificates_for_aggregator(session, aggregator_id)
        return mapper.CertificateMapper.map_to_page_response(
            total_count=cert_count, start=start, limit=limit, certificates=cert_list
        )

    async def add_many_certificates_for_aggregator(
        session: AsyncSession, aggregator_id: int, cert_list: list[schema.CertificateRequest]
    ) -> None:
        """Adds many certificates to an aggregator. Does nothing if no aggregator exists for given id."""
        cert_models = mapper.CertificateMapper.map_to_page_response
        
