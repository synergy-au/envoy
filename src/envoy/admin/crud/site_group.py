from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from envoy.server.model import SiteGroup, SiteGroupAssignment


async def fetch_site_group_id_restrictions(
    session: AsyncSession, site_id: int | None, group_name: str | None
) -> set[int] | None:
    """Admin function for fetching all SiteGroup.site_group_id in the db filtered by site_id / group name

    Can return None indicating that ALL site groups are in scope"""

    if site_id is None and group_name is None:
        return None

    stmt = select(SiteGroupAssignment.site_group_id)

    if group_name is not None:
        stmt = stmt.join(SiteGroup, SiteGroup.site_group_id == SiteGroupAssignment.site_group_id).where(
            SiteGroup.name == group_name
        )

    if site_id is not None:
        stmt = stmt.where(SiteGroupAssignment.site_id == site_id)

    return set((await session.execute(stmt)).scalars())
