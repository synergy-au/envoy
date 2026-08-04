from datetime import datetime

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from envoy.server.model.site import Site, SiteGroup, SiteGroupAssignment


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


def required_site_group_visible_to_site(
    required_site_group_id_col: InstrumentedAttribute[int | None], site_id: int
) -> ColumnElement[bool]:
    """Builds a filter clause for an optional "required_site_group_id" column (SiteControlGroup/Tariff): True if
    the column is NULL (no restriction - globally visible) or if site_id is a member (via SiteGroupAssignment) of
    the SiteGroup it references.

    Never joins against the enclosing statement, so an entity row can never fan out into multiple result rows
    regardless of how many sites are in the required SiteGroup."""

    member_exists = (
        select(SiteGroupAssignment.site_group_assignment_id)
        .where(
            (SiteGroupAssignment.site_group_id == required_site_group_id_col) & (SiteGroupAssignment.site_id == site_id)
        )
        .exists()
    )
    return or_(required_site_group_id_col.is_(None), member_exists)


async def assign_default_site_groups_to_site(session: AsyncSession, site_id: int, changed_time: datetime) -> None:
    """Adds a SiteGroupAssignment linking site_id to every SiteGroup marked as default_group=True. Intended to be
    called immediately after a new Site is created so it automatically becomes a member of the "default" groups.

    Does not flush/commit - it's expected the caller will do so as part of the enclosing transaction."""

    default_group_ids = (
        (await session.execute(select(SiteGroup.site_group_id).where(SiteGroup.default_group.is_(True))))
        .scalars()
        .all()
    )

    for site_group_id in default_group_ids:
        session.add(SiteGroupAssignment(site_id=site_id, site_group_id=site_group_id, changed_time=changed_time))
