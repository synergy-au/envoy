from sqlalchemy.ext.asyncio import AsyncSession
from envoy_schema.admin.schema.certificate import CertificatePageResponse

from envoy.admin import crud
from envoy.server import crud as server_crud
from envoy.server import exception
from envoy.admin import mapper


class CertificateManager:
    @staticmethod
    async def fetch_many_certificates_for_aggregator(
        session: AsyncSession, aggregator_id: int, start: int, limit: int
    ) -> CertificatePageResponse | None:
        """Select many certificates from the DB and map to a list of CertificateResponse objects"""
        # Assess aggregator exists
        if not await server_crud.aggregator.select_aggregator(session, aggregator_id):
            return None
        cert_list = await crud.certificate.select_all_certificates_for_aggregator(session, aggregator_id, start, limit)
        cert_count = await crud.certificate.count_certificates_for_aggregator(session, aggregator_id)
        return mapper.CertificateMapper.map_to_page_response(
            total_count=cert_count, start=start, limit=limit, certificates=cert_list
        )

    @staticmethod
    async def unassign_certificate_for_aggregator(
        session: AsyncSession, aggregator_id: int, certificate_id: int
    ) -> None:
        """Delete aggregator certificate assignment.

        Certificates are left untouched, only the join entry is deleted

        Args:
            aggregator_id: Aggregator that the certificates belong to
            certificate_ids: List of certificates to be unassigned. Does nothing if the relationship
                doesn't exist

        Raises:
            NotFoundError: if aggregator id or certificate id is invalid
        """
        if not await server_crud.aggregator.select_aggregator(session, aggregator_id):
            raise exception.NotFoundError(f"Aggregator with id {aggregator_id} not found")

        if not await crud.certificate.select_certificate(session, certificate_id):
            raise exception.NotFoundError(f"Certificate with id {certificate_id} not found")

        await crud.aggregator.unassign_many_certificates(session, aggregator_id, [certificate_id])
        await session.commit()
