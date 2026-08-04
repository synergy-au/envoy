from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from envoy.server.model.site import Site, SiteGroupAssignment


async def fetch_site_group_membership(
    session: AsyncSession, aggregator_id: int | None, site_id: int
) -> set[int] | None:
    """Enumerates all SiteGroup membership for site_id, returning every site_group_id that site is assigned to.

    If site_id is missing / doesn't match aggregator_id - returns empty set.

    aggregator_id: If set - scope the site lookup to ensure site_id belongs to aggregator_id

    returns the set (possibly empty) of site_group_id's that site belongs to OR None if site DNE"""

    if aggregator_id is None:
        result = await session.execute(
            select(SiteGroupAssignment.site_group_id).where(SiteGroupAssignment.site_id == site_id)
        )
    else:
        result = await session.execute(
            select(SiteGroupAssignment.site_group_id)
            .join(Site, Site.site_id == SiteGroupAssignment.site_id)
            .where((SiteGroupAssignment.site_id == site_id) & (Site.aggregator_id == aggregator_id))
        )

    result = set(result.scalars())

    if not result:
        # It's possible that site DNE - in this specific situation we want to return None instead of empty set
        site_exists_stmt = exists().where(Site.site_id == site_id)
        if aggregator_id is not None:
            site_exists_stmt = site_exists_stmt.where(Site.aggregator_id == aggregator_id)
        site_exists_result = (await session.execute(select(site_exists_stmt))).scalar_one_or_none()
        if not site_exists_result:
            return None

    return result
