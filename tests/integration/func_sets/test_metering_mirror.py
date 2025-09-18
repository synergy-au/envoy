import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Optional, Sequence

import envoy_schema.server.schema.uri as uris
import pytest
from assertical.asserts.generator import assert_class_instance_equality
from assertical.asserts.time import assert_nowish
from assertical.fixtures.postgres import generate_async_session
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorMeterReadingListRequest,
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
    MirrorUsagePointRequest,
)
from envoy_schema.server.schema.sep2.types import (
    AccumulationBehaviourType,
    DataQualifierType,
    FlowDirectionType,
    KindType,
    PhaseCode,
    ServiceKind,
    UomType,
)
from httpx import AsyncClient
from sqlalchemy import func, select

from envoy.server.model.archive.site_reading import ArchiveSiteReadingType
from envoy.server.model.site_reading import SiteReading, SiteReadingType
from tests.data.certificates.certificate1 import TEST_CERTIFICATE_FINGERPRINT as AGG_1_VALID_CERT
from tests.data.certificates.certificate4 import TEST_CERTIFICATE_FINGERPRINT as AGG_2_VALID_CERT
from tests.data.certificates.certificate6 import TEST_CERTIFICATE_FINGERPRINT as DEVICE_5_CERT
from tests.data.certificates.certificate6 import TEST_CERTIFICATE_LFDI as DEVICE_5_LFDI
from tests.data.certificates.certificate7 import TEST_CERTIFICATE_FINGERPRINT as DEVICE_6_CERT
from tests.data.certificates.certificate8 import TEST_CERTIFICATE_FINGERPRINT as UNREGISTERED_CERT
from tests.integration.integration_server import cert_header
from tests.integration.request import build_paging_params
from tests.integration.response import (
    assert_error_response,
    assert_response_header,
    read_location_header,
    read_response_body_string,
)

HREF_PREFIX = "/prefix"


@pytest.mark.parametrize(
    "start, changed_after, limit, cert, expected_count, expected_mup_hrefs",
    [
        # Testing start / limit
        (0, None, 99, AGG_1_VALID_CERT, 3, ["/mup/1", "/mup/3", "/mup/4"]),
        (1, None, 99, AGG_1_VALID_CERT, 3, ["/mup/3", "/mup/4"]),
        (2, None, 99, AGG_1_VALID_CERT, 3, ["/mup/4"]),
        (3, None, 99, AGG_1_VALID_CERT, 3, []),
        (0, None, 2, AGG_1_VALID_CERT, 3, ["/mup/1", "/mup/3"]),
        (1, None, 1, AGG_1_VALID_CERT, 3, ["/mup/3"]),
        # Changed time
        (
            0,
            datetime(2022, 5, 6, 11, 22, 30, tzinfo=timezone.utc),
            99,
            AGG_1_VALID_CERT,
            3,
            ["/mup/1", "/mup/3", "/mup/4"],
        ),
        (
            0,
            datetime(2022, 5, 6, 11, 22, 35, tzinfo=timezone.utc),
            99,
            AGG_1_VALID_CERT,
            3,
            ["/mup/1", "/mup/3", "/mup/4"],
        ),  # One of the grouped items in /mup/1 has a changed time in the future
        (0, datetime(2022, 5, 6, 13, 22, 35, tzinfo=timezone.utc), 99, AGG_1_VALID_CERT, 2, ["/mup/1", "/mup/4"]),
        (0, datetime(2022, 5, 6, 15, 22, 35, tzinfo=timezone.utc), 99, AGG_1_VALID_CERT, 0, []),
        (1, datetime(2022, 5, 6, 13, 22, 36, tzinfo=timezone.utc), 2, AGG_1_VALID_CERT, 2, ["/mup/4"]),
        # Changed cert
        (0, None, 99, AGG_2_VALID_CERT, 0, []),
        (0, None, 99, DEVICE_5_CERT, 0, []),
        (0, None, 99, UNREGISTERED_CERT, 0, []),  # An unregistered device should still return an empty list
    ],
)
@pytest.mark.anyio
async def test_get_mirror_usage_point_list_pagination(
    client: AsyncClient,
    start: Optional[int],
    changed_after: Optional[datetime],
    limit: Optional[int],
    cert: str,
    expected_count: int,
    expected_mup_hrefs: list[int],
):
    """Simple test of pagination of MUPs for a given aggregator"""
    response = await client.get(
        uris.MirrorUsagePointListUri + build_paging_params(limit=limit, start=start, changed_after=changed_after),
        headers={cert_header: urllib.parse.quote(cert)},
    )
    assert_response_header(response, HTTPStatus.OK)
    body = read_response_body_string(response)
    assert len(body) > 0
    parsed_response: MirrorUsagePointListResponse = MirrorUsagePointListResponse.from_xml(body)
    assert parsed_response.all_ == expected_count, f"received body:\n{body}"
    assert parsed_response.results == len(expected_mup_hrefs), f"received body:\n{body}"

    if len(expected_mup_hrefs) > 0:
        assert parsed_response.mirrorUsagePoints, f"received body:\n{body}"
        assert [mup.href for mup in parsed_response.mirrorUsagePoints] == expected_mup_hrefs
    else:
        assert (
            parsed_response.mirrorUsagePoints is None or len(parsed_response.mirrorUsagePoints) == 0
        ), f"received body:\n{body}"


@pytest.mark.parametrize(
    "mup, expected_href, expected_status",
    [
        # Create a new mup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "123",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "01",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "123abc",
                            "readingType": {
                                "powerOfTenMultiplier": 5,
                                "kind": KindType.POWER,
                                "uom": UomType.DISPLACEMENT_POWER_FACTOR_COSTHETA,
                                "phase": PhaseCode.PHASE_CA,
                                "flowDirection": FlowDirectionType.REVERSE,
                            },
                        }
                    ],
                }
            ),
            "/mup/7",
            HTTPStatus.CREATED,
        ),
        # Create a new mup (uppercase lfdi)
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "123",
                    "deviceLFDI": "SITE1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "01",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "123abc",
                            "readingType": {
                                "powerOfTenMultiplier": 5,
                                "kind": KindType.POWER,
                                "uom": UomType.DISPLACEMENT_POWER_FACTOR_COSTHETA,
                                "phase": PhaseCode.PHASE_CA,
                                "flowDirection": FlowDirectionType.REVERSE,
                            },
                        }
                    ],
                }
            ),
            "/mup/7",
            HTTPStatus.CREATED,
        ),
        # Create a new mup with powerOfTenMultiplier = 0
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "123",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "7a",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "123abc",
                            "readingType": {
                                "powerOfTenMultiplier": 0,
                                "kind": KindType.POWER,
                                "uom": UomType.DISPLACEMENT_POWER_FACTOR_COSTHETA,
                                "phase": PhaseCode.PHASE_CA,
                                "flowDirection": FlowDirectionType.REVERSE,
                            },
                        }
                    ],
                }
            ),
            "/mup/7",
            HTTPStatus.CREATED,
        ),
        # Update an existing mup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "10000000000000000000000000000def",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "18",
                    "status": 0,
                    "version": 76,
                    "description": "MUP Description Update",
                    "mirrorMeterReadings": [
                        {
                            "mRID": "new-mmr",
                            "readingType": {
                                "powerOfTenMultiplier": 3,
                                "kind": KindType.POWER,
                                "uom": UomType.REAL_POWER_WATT,
                                "phase": PhaseCode.PHASE_B,
                                "flowDirection": FlowDirectionType.FORWARD,
                                "dataQualifier": DataQualifierType.AVERAGE,
                                "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                                "intervalLength": None,
                            },
                        }
                    ],
                }
            ),
            "/mup/1",
            HTTPStatus.NO_CONTENT,
        ),
        # Update an existing mup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "10000000000000000000000000000def",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "18",
                    "status": 1,
                    "version": 99,
                    "description": "MUP Description",
                    "mirrorMeterReadings": [
                        {
                            "mRID": "new-mmr",
                            "readingType": {
                                "powerOfTenMultiplier": 3,
                                "kind": KindType.POWER,
                                "uom": UomType.REAL_POWER_WATT,
                                "phase": PhaseCode.PHASE_B,
                                "flowDirection": FlowDirectionType.FORWARD,
                                "dataQualifier": DataQualifierType.AVERAGE,
                                "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                                "intervalLength": None,
                            },
                        }
                    ],
                }
            ),
            "/mup/1",
            HTTPStatus.NO_CONTENT,
        ),
        # Case insensitive lfdi lookup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "10000000000000000000000000000def",
                    "deviceLFDI": "site1-LFDI",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "18",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "new-mmr",
                            "readingType": {
                                "powerOfTenMultiplier": 3,
                                "kind": KindType.POWER,
                                "uom": UomType.REAL_POWER_WATT,
                                "phase": PhaseCode.PHASE_B,
                                "flowDirection": FlowDirectionType.FORWARD,
                                "dataQualifier": DataQualifierType.AVERAGE,
                                "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                                "intervalLength": None,
                            },
                        }
                    ],
                }
            ),
            "/mup/1",
            HTTPStatus.NO_CONTENT,
        ),
    ],
)
@pytest.mark.anyio
async def test_create_update_mup(
    client: AsyncClient, mup: MirrorUsagePointRequest, expected_href: str, expected_status: HTTPStatus
):
    """Tests creating/updating a mup and seeing if the updates stick and can be fetched via list/direct requests"""
    now = datetime.now(tz=timezone.utc)

    # create/update the mup
    response = await client.post(
        uris.MirrorUsagePointListUri,
        content=MirrorUsagePointRequest.to_xml(mup, skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, expected_status, expected_content_type=None)
    assert read_location_header(response) == expected_href

    # see if we can fetch the mup directly
    response = await client.get(expected_href, headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)})
    assert_response_header(response, HTTPStatus.OK)
    body = read_response_body_string(response)
    assert len(body) > 0
    parsed_response: MirrorUsagePoint = MirrorUsagePoint.from_xml(body)
    assert parsed_response.href == expected_href
    assert_class_instance_equality(
        MirrorUsagePoint, mup, parsed_response, ignored_properties=set(["href", "mRID", "postRate", "deviceLFDI"])
    )
    assert parsed_response.deviceLFDI.lower() == mup.deviceLFDI.lower(), "The returned LFDI should be case insensitive"

    # see if the list endpoint can fetch it via the updated time
    response = await client.get(
        uris.MirrorUsagePointListUri + build_paging_params(limit=99, changed_after=now),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, HTTPStatus.OK)
    body = read_response_body_string(response)
    assert len(body) > 0
    parsed_list_response: MirrorUsagePointListResponse = MirrorUsagePointListResponse.from_xml(body)
    assert parsed_list_response.results == 1, f"received body:\n{body}"
    assert [mup.href for mup in parsed_list_response.mirrorUsagePoints] == [expected_href]


@pytest.mark.parametrize(
    "mup, expected_href, expected_status",
    [
        # Create a new mup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "123",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "0",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "123abc",
                            "readingType": {
                                "powerOfTenMultiplier": 5,
                                "kind": KindType.POWER,
                                "uom": UomType.DISPLACEMENT_POWER_FACTOR_COSTHETA,
                                "phase": PhaseCode.PHASE_CA,
                                "flowDirection": FlowDirectionType.REVERSE,
                            },
                        }
                    ],
                }
            ),
            HREF_PREFIX + "/mup/7",
            HTTPStatus.CREATED,
        ),
        # Update an existing mup
        (
            MirrorUsagePointRequest.model_validate(
                {
                    "mRID": "10000000000000000000000000000def",
                    "deviceLFDI": "site1-lfdi",
                    "serviceCategoryKind": ServiceKind.ELECTRICITY,
                    "roleFlags": "1",
                    "status": 0,
                    "mirrorMeterReadings": [
                        {
                            "mRID": "50000000000000000000000000000abc",
                            "readingType": {
                                "powerOfTenMultiplier": 3,
                                "kind": KindType.POWER,
                                "uom": UomType.REAL_POWER_WATT,
                                "phase": PhaseCode.PHASE_B,
                                "flowDirection": FlowDirectionType.FORWARD,
                                "dataQualifier": DataQualifierType.AVERAGE,
                                "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                                "intervalLength": None,
                            },
                        }
                    ],
                }
            ),
            HREF_PREFIX + "/mup/1",
            HTTPStatus.NO_CONTENT,
        ),
    ],
)
@pytest.mark.anyio
@pytest.mark.href_prefix(HREF_PREFIX)
async def test_create_update_mup_href_prefix(
    client: AsyncClient, mup: MirrorUsagePointRequest, expected_href: str, expected_status: HTTPStatus
):
    """Tests creating/updating a mup and seeing if the updates stick and can be fetched via list/direct requests"""
    # create/update the mup
    response = await client.post(
        uris.MirrorUsagePointListUri,
        content=MirrorUsagePointRequest.to_xml(mup, skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, expected_status, expected_content_type=None)
    assert read_location_header(response) == expected_href


@pytest.mark.parametrize(
    "min_val, max_val",
    [
        (9, -10),  # Normal values
        (int("FFFFFFFFFFFF", 16), -int("FFFFFFFFFFFF", 16)),  # int48 max/min values (sep2 uses int48 value range)
        (0, 0),  # zero values
    ],
)
@pytest.mark.anyio
async def test_submit_mirror_meter_reading(client: AsyncClient, pg_base_config, min_val, max_val):
    """Submits a batch of readings to a mup and checks the DB to see if they are created"""
    mmr: MirrorMeterReading = MirrorMeterReading.model_validate(
        {
            "mRID": "10000000000000000000000000000abc",
            "mirrorReadingSets": [
                {
                    "mRID": "1234abc",
                    "timePeriod": {
                        "duration": 603,
                        "start": 1341579365,
                    },
                    "readings": [
                        {"value": max_val, "timePeriod": {"duration": 301, "start": 1341579365}, "localID": "123"},
                        {"value": min_val, "timePeriod": {"duration": 302, "start": 1341579666}, "localID": "0f0d"},
                    ],
                }
            ],
        }
    )
    mup_id = 1

    # submit the readings
    response = await client.post(
        uris.MirrorUsagePointUri.format(mup_id=mup_id),
        content=MirrorMeterReading.to_xml(mmr, skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, HTTPStatus.CREATED, expected_content_type=None)

    # validate the DB directly
    async with generate_async_session(pg_base_config) as session:
        stmt = select(SiteReading).order_by(SiteReading.site_reading_id)

        resp = await session.execute(stmt)
        all_readings: Sequence[SiteReading] = resp.scalars().all()

        assert len(all_readings) == 6, "We should've added 2 readings"
        assert all_readings[-2].site_reading_type_id == mup_id
        assert all_readings[-2].local_id == int("123", base=16)
        assert all_readings[-2].value == max_val
        assert all_readings[-2].time_period_seconds == 301
        assert all_readings[-2].time_period_start == datetime.fromtimestamp(1341579365, tz=timezone.utc)
        assert_nowish(all_readings[-2].changed_time)

        assert all_readings[-1].site_reading_type_id == mup_id
        assert all_readings[-1].local_id == int("0f0d", base=16)
        assert all_readings[-1].value == min_val
        assert all_readings[-1].time_period_seconds == 302
        assert all_readings[-1].time_period_start == datetime.fromtimestamp(1341579666, tz=timezone.utc)
        assert_nowish(all_readings[-1].changed_time)


@pytest.mark.parametrize(
    "min_val, max_val",
    [
        (9, -10),  # Normal values
        (int("FFFFFFFFFFFF", 16), -int("FFFFFFFFFFFF", 16)),  # int48 max/min values (sep2 uses int48 value range)
        (0, 0),  # zero values
    ],
)
@pytest.mark.anyio
async def test_submit_mirror_meter_reading_single_value(client: AsyncClient, pg_base_config, min_val, max_val):
    """Submits a reading without a MirrorReadingSet to a mup and check the DB to see if it is created"""
    mmr: MirrorMeterReading = MirrorMeterReading.model_validate(
        {
            "mRID": "10000000000000000000000000000abc",
            "reading": {"value": max_val, "timePeriod": {"duration": 301, "start": 1341579365}, "localID": "123"},
        }
    )
    mup_id = 1

    # submit the readings
    response = await client.post(
        uris.MirrorUsagePointUri.format(mup_id=mup_id),
        content=MirrorMeterReading.to_xml(mmr, skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, HTTPStatus.CREATED, expected_content_type=None)

    # validate the DB directly
    async with generate_async_session(pg_base_config) as session:
        stmt = select(SiteReading).order_by(SiteReading.site_reading_id)

        resp = await session.execute(stmt)
        all_readings: Sequence[SiteReading] = resp.scalars().all()

        assert len(all_readings) == 5, "We should have added 1 reading"
        assert all_readings[-1].site_reading_type_id == mup_id
        assert all_readings[-1].local_id == int("123", base=16)
        assert all_readings[-1].value == max_val
        assert all_readings[-1].time_period_seconds == 301
        assert all_readings[-1].time_period_start == datetime.fromtimestamp(1341579365, tz=timezone.utc)
        assert_nowish(all_readings[-1].changed_time)


@pytest.mark.anyio
async def test_submit_multiple_mirror_meter_readings(client: AsyncClient, pg_base_config):
    """Submits a reading without a MirrorReadingSet to a mup and check the DB to see if it is created"""

    mmrs: MirrorMeterReadingListRequest = MirrorMeterReadingListRequest.model_validate(
        {
            "mirrorMeterReadings": [
                {
                    "mRID": "50000000000000000000000000000abc",
                    "reading": {
                        "value": 456,
                        "timePeriod": {"duration": 301, "start": 1341579365},
                        "localID": "123",
                    },
                },
                {
                    "mRID": "newmrid",
                    "reading": {
                        "value": 789,
                        "timePeriod": {"duration": 302, "start": 1341579366},
                        "localID": "456",
                    },
                    "readingType": {
                        "powerOfTenMultiplier": 4,
                        "kind": KindType.POWER,
                        "uom": UomType.REAL_POWER_WATT,
                        "phase": PhaseCode.PHASE_B,
                        "flowDirection": FlowDirectionType.REVERSE,
                        "dataQualifier": DataQualifierType.AVERAGE,
                        "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                        "intervalLength": None,
                    },
                },
            ]
        }
    )
    mup_id = 1

    # submit the readings
    response = await client.post(
        uris.MirrorUsagePointUri.format(mup_id=mup_id),
        content=mmrs.to_xml(skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, HTTPStatus.CREATED, expected_content_type=None)

    # validate the DB directly
    async with generate_async_session(pg_base_config) as session:
        stmt = select(SiteReading).order_by(SiteReading.site_reading_id)

        resp = await session.execute(stmt)
        all_readings: Sequence[SiteReading] = resp.scalars().all()

        assert len(all_readings) == 6, "We should have added 2 readings"

        # Our existing SiteReading that received a new reading
        assert all_readings[-2].site_reading_type_id == 5, "Our existing SiteReadingType"
        assert all_readings[-2].local_id == int("123", base=16)
        assert all_readings[-2].value == 456
        assert all_readings[-2].time_period_seconds == 301
        assert all_readings[-2].time_period_start == datetime.fromtimestamp(1341579365, tz=timezone.utc)
        assert_nowish(all_readings[-2].changed_time)

        # The new site reading type
        assert all_readings[-1].site_reading_type_id > 5, "Our new SiteReadingType"
        assert all_readings[-1].local_id == int("456", base=16)
        assert all_readings[-1].value == 789
        assert all_readings[-1].time_period_seconds == 302
        assert all_readings[-1].time_period_start == datetime.fromtimestamp(1341579366, tz=timezone.utc)
        assert_nowish(all_readings[-1].changed_time)

        new_srt = (
            await session.execute(
                select(SiteReadingType).where(
                    SiteReadingType.site_reading_type_id == all_readings[-1].site_reading_type_id
                )
            )
        ).scalar_one()
        assert_nowish(new_srt.created_time)
        assert_nowish(new_srt.changed_time)
        assert new_srt.power_of_ten_multiplier == 4
        assert new_srt.flow_direction == FlowDirectionType.REVERSE
        assert new_srt.phase == PhaseCode.PHASE_B
        assert new_srt.site_id == 1
        assert new_srt.group_id == 1
        assert new_srt.group_mrid == "10000000000000000000000000000def"


@pytest.mark.anyio
async def test_submit_mirror_meter_reading_no_reading_type(client: AsyncClient):
    """Submits a reading without MirrorMeterReading readingType to a new MMR"""
    mmr: MirrorMeterReading = MirrorMeterReading.model_validate(
        {
            "mRID": "abc123",
            "reading": {"value": 1, "timePeriod": {"duration": 301, "start": 1341579365}, "localID": "123"},
        }
    )
    mup_id = 1

    # submit the readings
    response = await client.post(
        uris.MirrorUsagePointUri.format(mup_id=mup_id),
        content=MirrorMeterReading.to_xml(mmr, skip_empty=False, exclude_none=True, exclude_unset=True),
        headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)},
    )
    assert_response_header(response, HTTPStatus.BAD_REQUEST, expected_content_type=None)
    assert_error_response(response)


@pytest.mark.anyio
async def test_device_cert_mup_creation(client: AsyncClient):
    """Tests running through the MUP create/fetch flow with a device cert"""
    mup = MirrorUsagePointRequest.model_validate(
        {
            "mRID": "456",
            "deviceLFDI": DEVICE_5_LFDI,
            "serviceCategoryKind": ServiceKind.ELECTRICITY,
            "roleFlags": "00",
            "status": 0,
            "mirrorMeterReadings": [
                {
                    "mRID": "456abc",
                    "readingType": {
                        "powerOfTenMultiplier": 3,
                        "kind": KindType.POWER,
                        "uom": UomType.REAL_POWER_WATT,
                        "phase": PhaseCode.PHASE_B,
                        "flowDirection": FlowDirectionType.FORWARD,
                        "dataQualifier": DataQualifierType.AVERAGE,
                        "accumulationBehaviour": AccumulationBehaviourType.CUMULATIVE,
                        "intervalLength": None,
                    },
                }
            ],
        }
    )

    # create/update the mup
    response = await client.post(
        uris.MirrorUsagePointListUri,
        content=MirrorUsagePointRequest.to_xml(mup, skip_empty=True),
        headers={cert_header: urllib.parse.quote(DEVICE_5_CERT)},
    )
    assert_response_header(response, HTTPStatus.CREATED, expected_content_type=None)
    mup_href = read_location_header(response)

    # see if we can fetch the mup directly
    response = await client.get(mup_href, headers={cert_header: urllib.parse.quote(DEVICE_5_CERT)})
    assert_response_header(response, HTTPStatus.OK)
    body = read_response_body_string(response)
    assert len(body) > 0
    parsed_response: MirrorUsagePoint = MirrorUsagePoint.from_xml(body)
    assert parsed_response.href == mup_href
    assert_class_instance_equality(
        MirrorUsagePoint, mup, parsed_response, ignored_properties=set(["href", "mRID", "postRate"])
    )

    # Ensure other certs can't access it
    response = await client.get(mup_href, headers={cert_header: urllib.parse.quote(DEVICE_6_CERT)})
    assert_response_header(response, HTTPStatus.NOT_FOUND)
    assert_error_response(response)

    response = await client.get(mup_href, headers={cert_header: urllib.parse.quote(UNREGISTERED_CERT)})
    assert_response_header(response, HTTPStatus.FORBIDDEN)
    assert_error_response(response)

    response = await client.get(mup_href, headers={cert_header: urllib.parse.quote(AGG_1_VALID_CERT)})
    assert_response_header(response, HTTPStatus.NOT_FOUND)
    assert_error_response(response)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "cert, mup_id, expected_status",
    [
        (AGG_1_VALID_CERT, 1, HTTPStatus.NO_CONTENT),
        (AGG_1_VALID_CERT, 3, HTTPStatus.NO_CONTENT),
        (AGG_1_VALID_CERT, 0, HTTPStatus.NOT_FOUND),  # mup DNE
        (AGG_1_VALID_CERT, 99, HTTPStatus.NOT_FOUND),  # mup DNE
        (AGG_1_VALID_CERT, 2, HTTPStatus.NOT_FOUND),  # Wrong Aggregator
        (AGG_2_VALID_CERT, 1, HTTPStatus.NOT_FOUND),  # Wrong Aggregator
    ],
)
async def test_delete_mup(
    pg_base_config,
    client: AsyncClient,
    cert: str,
    mup_id: int,
    expected_status: HTTPStatus,
):
    """Tests that deleting named end device's works / fails in simple cases"""

    uri = uris.MirrorUsagePointUri.format(mup_id=mup_id)
    response = await client.delete(uri, headers={cert_header: urllib.parse.quote(cert)})
    assert_response_header(response, expected_status, expected_content_type=None)

    if response.status_code == HTTPStatus.NO_CONTENT:
        # If the delete succeeded - fire off a get to the same resource to see if it now 404's
        response = await client.get(uri, headers={cert_header: urllib.parse.quote(cert)})
        assert_response_header(response, HTTPStatus.NOT_FOUND)

        # Won't exhaustively check archive use - but sanity check that the archive is receiving records
        async with generate_async_session(pg_base_config) as session:
            site_archive_count = (
                await session.execute(select(func.count()).select_from(ArchiveSiteReadingType))
            ).scalar_one()
            assert site_archive_count > 0, "Validate that the archive functionality was utilised"
    else:
        assert response.status_code in [HTTPStatus.NOT_FOUND, HTTPStatus.FORBIDDEN]

        # Won't exhaustively check archive use - but sanity check that the archive is NOT receiving records
        async with generate_async_session(pg_base_config) as session:
            site_archive_count = (
                await session.execute(select(func.count()).select_from(ArchiveSiteReadingType))
            ).scalar_one()
            assert site_archive_count == 0, "Nothing should be archived"
