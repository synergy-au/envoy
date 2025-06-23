import itertools

from sqlalchemy.ext.asyncio import AsyncSession
from envoy_schema.admin.schema.certificate import CertificatePageResponse, CertificateAssignmentRequest

from envoy.admin import crud
from envoy.server import crud as server_crud
from envoy.admin import mapper
from envoy.server.model.base import Certificate


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
    async def add_many_certificates_for_aggregator(
        session: AsyncSession, aggregator_id: int, certs: list[CertificateAssignmentRequest]
    ) -> None:
        """Create certificates if they don't exist ignore if they do and assign to aggregrator

        Will create the certificate first if needed. If an expiry is provided on a certificate that exists already,
        then it is ignored and the original expiry is maintained.

        Args:
            session: Database session
            aggregator_id: ID of aggregator in database to assign certs to
            certs: Partial/Full certs to be assigned. For new certs all fields excluding id must be supplied

        Raises:
            LookupError: if aggregator id is invalid
            ReferenceError: if a certificate id supplied doesn't already exist
        """
        # Assess aggregator exists
        if not await server_crud.aggregator.select_aggregator(session, aggregator_id):
            raise LookupError(f"Aggregator with id {aggregator_id} not found")

        # Perform mapping
        mapped_certs = mapper.CertificateMapper.map_from_many_request(certs)
        mapped_cert_ids = [mc.certificate_id for mc in mapped_certs if mc.certificate_id is not None]

        # Filter existing
        existing = await crud.certificate.select_many_certificates_by_id_or_lfdi(session, mapped_certs)
        existing_ids = [e.certificate_id for e in existing]

        # If an id doesn't exist then raise
        for mc_id in mapped_cert_ids:
            if mc_id not in existing_ids:
                raise ReferenceError(f"Certificate with id {mc_id} does not exist")

        # Filter to be created
        certs_to_create = [c for c in mapped_certs if c.certificate_id is None]

        # Create new ignore existing lfdis
        new_certs = await crud.certificate.create_many_certificates_on_conflict_do_nothing(session, certs_to_create)

        # Assign all to aggregator
        new_cert_ids = (c.certificate_id for c in new_certs)
        all_ids = itertools.chain(new_cert_ids, mapped_cert_ids)

        # Ensure something to assign otherwise return
        peeker, producer = itertools.tee(all_ids)
        try:
            next(peeker)
        except StopIteration:
            return

        await crud.aggregator.assign_many_certificates(session, aggregator_id, producer)
        await session.commit()
