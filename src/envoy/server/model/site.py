from datetime import datetime
from decimal import Decimal
from typing import Optional

from envoy_schema.server.schema.sep2.der import (
    AbnormalCategoryType,
    AlarmStatusType,
    ConnectStatusType,
    DERControlType,
    DERType,
    DOESupportedMode,
    InverterStatusType,
    LocalControlModeStatusType,
    NormalCategoryType,
    OperationalModeStatusType,
    StorageModeStatusType,
)
from envoy_schema.server.schema.sep2.log_events import FunctionSetIdentifier, ProfileIdentifier
from envoy_schema.server.schema.sep2.types import DeviceCategory
from sqlalchemy import (
    DECIMAL,
    INTEGER,
    SMALLINT,
    VARCHAR,
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from envoy.server.model import Base
from envoy.server.model.constants import DOE_DECIMAL_PLACES, PERCENT_DECIMAL_PLACES


class Site(Base):
    __tablename__ = "site"

    site_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nmi: Mapped[Optional[str]] = mapped_column(VARCHAR(length=11), nullable=True)
    aggregator_id: Mapped[int] = mapped_column(ForeignKey("aggregator.aggregator_id"), nullable=False)

    timezone_id: Mapped[str] = mapped_column(VARCHAR(length=64), nullable=False)  # tz_id name of the local timezone
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)  # When the site was updated
    lfdi: Mapped[str] = mapped_column(VARCHAR(length=42), nullable=False, unique=True)
    sfdi: Mapped[int] = mapped_column(BigInteger, nullable=False)
    device_category: Mapped[DeviceCategory] = mapped_column(INTEGER, nullable=False)
    registration_pin: Mapped[int] = mapped_column(
        INTEGER, nullable=False
    )  # 5 digit PIN, randomly generated on creation. Used for out of band confirmations.

    assignments: Mapped[list["SiteGroupAssignment"]] = relationship(
        back_populates="site",
        lazy="raise",
        cascade="all, delete",
        passive_deletes=True,
    )  # What assignments reference this group

    site_ders: Mapped[list["SiteDER"]] = relationship(
        back_populates="site",
        lazy="raise",
        cascade="all, delete",
        passive_deletes=True,
    )  # What DER live underneath/behind this site
    default_site_control: Mapped[Optional["DefaultSiteControl"]] = relationship(
        back_populates="site", lazy="raise", passive_deletes=True, uselist=False
    )  # The default DOE + other controls that apply to this site

    # NOTE: We're defining Default are set on a per Site basis

    __table_args__ = (
        UniqueConstraint("sfdi", "aggregator_id", name="sfdi_aggregator_id_uc"),
        UniqueConstraint("lfdi", "aggregator_id", name="lfdi_aggregator_id_uc"),  # Mirror Metering requires unique lfdi
    )


class SiteGroup(Base):
    """Site groups are a way of logically grouping Sites independent of their aggregator/other details. Each Site may
    belong to multiple groups and its expected that these groupings are managed via the admin server"""

    __tablename__ = "site_group"

    site_group_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    name: Mapped[str] = mapped_column(VARCHAR(length=128))  # Name/Title of this group - must be unique
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site group was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    assignments: Mapped[list["SiteGroupAssignment"]] = relationship(
        back_populates="group",
        lazy="raise",
        cascade="all, delete",
        passive_deletes=True,
    )  # What assignments reference this group

    __table_args__ = (UniqueConstraint("name", name="name_uc"),)


class SiteGroupAssignment(Base):
    """Provides a many-many mapping between Site and SiteGroup"""

    __tablename__ = "site_group_assignment"

    site_group_assignment_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    created_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    site_id: Mapped[int] = mapped_column(ForeignKey("site.site_id", ondelete="CASCADE"))
    site_group_id: Mapped[int] = mapped_column(ForeignKey("site_group.site_group_id", ondelete="CASCADE"))

    site: Mapped["Site"] = relationship(back_populates="assignments", lazy="raise")
    group: Mapped["SiteGroup"] = relationship(back_populates="assignments", lazy="raise")

    # We don't want a single site to be linked to a group multiple times
    __table_args__ = (UniqueConstraint("site_id", "site_group_id", name="site_id_site_group_id_uc"),)


class SiteDER(Base):
    """Represents a Distributed Energy Resource behind a Site's connection point - primarily a repository
    of metadata / ratings"""

    __tablename__ = "site_der"

    site_der_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("site.site_id", ondelete="CASCADE"))

    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site DER was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    site: Mapped["Site"] = relationship(back_populates="site_ders", lazy="raise")
    site_der_rating: Mapped[Optional["SiteDERRating"]] = relationship(
        back_populates="site_der", uselist=False, lazy="raise"
    )
    site_der_setting: Mapped[Optional["SiteDERSetting"]] = relationship(
        back_populates="site_der", uselist=False, lazy="raise"
    )
    site_der_availability: Mapped[Optional["SiteDERAvailability"]] = relationship(
        back_populates="site_der", uselist=False, lazy="raise"
    )
    site_der_status: Mapped[Optional["SiteDERStatus"]] = relationship(
        back_populates="site_der", uselist=False, lazy="raise"
    )


class SiteDERRating(Base):
    """Represents the nameplate rating values associated with a SiteDER. These are not expected to change
    after initially being set (excepting erroneous assignments). Only a single SiteDERRating should be assigned
    to a SiteDER"""

    __tablename__ = "site_der_rating"

    site_der_rating_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_der_id: Mapped[int] = mapped_column(ForeignKey("site_der.site_der_id", ondelete="CASCADE"))
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site DERRating was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # These values correspond to a flattened version of sep2 DERCapability
    modes_supported: Mapped[Optional[DERControlType]] = mapped_column(INTEGER, nullable=True)
    abnormal_category: Mapped[Optional[AbnormalCategoryType]] = mapped_column(INTEGER, nullable=True)
    max_a_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_a_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_ah_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_ah_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_v_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_v_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_neg_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_neg_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_w_value: Mapped[int] = mapped_column(INTEGER)
    max_w_multiplier: Mapped[int] = mapped_column(INTEGER)
    max_wh_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_wh_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_over_excited_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_over_excited_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_under_excited_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_under_excited_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_v_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_v_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    normal_category: Mapped[Optional[NormalCategoryType]] = mapped_column(INTEGER, nullable=True)
    over_excited_pf_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    over_excited_pf_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    over_excited_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    over_excited_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    reactive_susceptance_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    reactive_susceptance_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    under_excited_pf_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    under_excited_pf_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    under_excited_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    under_excited_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_nom_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_nom_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    der_type: Mapped[DERType] = mapped_column(INTEGER)
    doe_modes_supported: Mapped[Optional[DOESupportedMode]] = mapped_column(INTEGER, nullable=True)

    site_der: Mapped["SiteDER"] = relationship(back_populates="site_der_rating", lazy="raise", single_parent=True)
    __table_args__ = (UniqueConstraint("site_der_id"),)  # Only one SiteDERRating allowed per SiteDER)


class SiteDERSetting(Base):
    """Represents the current setting values associated with a SiteDER. The SiteDERRating represents
    the ratings/limits while the settings represent the currently enabled functionality"""

    __tablename__ = "site_der_setting"

    site_der_setting_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_der_id: Mapped[int] = mapped_column(ForeignKey("site_der.site_der_id", ondelete="CASCADE"))
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site DERSetting was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # These values correspond to a flattened version of sep2 DERSettings
    modes_enabled: Mapped[Optional[DERControlType]] = mapped_column(INTEGER, nullable=True)
    es_delay: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_high_freq: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_high_volt: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_low_freq: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_low_volt: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_ramp_tms: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    es_random_delay: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    grad_w: Mapped[int] = mapped_column(INTEGER)
    max_a_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_a_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_ah_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_ah_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_rate_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_w_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_discharge_rate_w_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_v_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_v_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_va_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_va_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_neg_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_var_neg_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_w_value: Mapped[int] = mapped_column(INTEGER)
    max_w_multiplier: Mapped[int] = mapped_column(INTEGER)
    max_wh_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_wh_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_over_excited_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_over_excited_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_under_excited_displacement: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_pf_under_excited_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_v_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    min_v_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    soft_grad_w: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_nom_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_nom_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_ref_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_ref_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_ref_ofs_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    v_ref_ofs_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    doe_modes_enabled: Mapped[Optional[DOESupportedMode]] = mapped_column(INTEGER, nullable=True)

    site_der: Mapped["SiteDER"] = relationship(back_populates="site_der_setting", lazy="raise", single_parent=True)
    __table_args__ = (UniqueConstraint("site_der_id"),)  # Only one SiteDERSetting allowed per SiteDER)


class SiteDERAvailability(Base):
    """Represents the current availability values associated with a SiteDER. Typically used for communicating
    the current snapshot of DER energy held in reserve"""

    __tablename__ = "site_der_availability"

    site_der_availability_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_der_id: Mapped[int] = mapped_column(ForeignKey("site_der.site_der_id", ondelete="CASCADE"))
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the SiteDERAvailability was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # These values correspond to a flattened version of sep2 DERAvailability
    availability_duration_sec: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    max_charge_duration_sec: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    reserved_charge_percent: Mapped[Optional[Decimal]] = mapped_column(
        DECIMAL(8, PERCENT_DECIMAL_PLACES), nullable=True
    )  # Needs to
    reserved_deliver_percent: Mapped[Optional[Decimal]] = mapped_column(
        DECIMAL(8, PERCENT_DECIMAL_PLACES), nullable=True
    )
    estimated_var_avail_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    estimated_var_avail_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    estimated_w_avail_value: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    estimated_w_avail_multiplier: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)

    site_der: Mapped["SiteDER"] = relationship(back_populates="site_der_availability", lazy="raise", single_parent=True)
    __table_args__ = (UniqueConstraint("site_der_id"),)  # Only one SiteDERSetting allowed per SiteDER)


class SiteDERStatus(Base):
    """Represents the current status values associated with a SiteDER. Typically used for communicating
    the current snapshot of DER status"""

    __tablename__ = "site_der_status"

    site_der_status_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_der_id: Mapped[int] = mapped_column(ForeignKey("site_der.site_der_id", ondelete="CASCADE"))
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the site DERStatus was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # These values correspond to a flattened version of sep2 DERStatus
    alarm_status: Mapped[Optional[AlarmStatusType]] = mapped_column(INTEGER, nullable=True)
    generator_connect_status: Mapped[Optional[ConnectStatusType]] = mapped_column(INTEGER, nullable=True)
    generator_connect_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    inverter_status: Mapped[Optional[InverterStatusType]] = mapped_column(INTEGER, nullable=True)
    inverter_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    local_control_mode_status: Mapped[Optional[LocalControlModeStatusType]] = mapped_column(INTEGER, nullable=True)
    local_control_mode_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    manufacturer_status: Mapped[Optional[str]] = mapped_column(VARCHAR(6), nullable=True)
    manufacturer_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    operational_mode_status: Mapped[Optional[OperationalModeStatusType]] = mapped_column(INTEGER, nullable=True)
    operational_mode_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    state_of_charge_status: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    state_of_charge_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    storage_mode_status: Mapped[Optional[StorageModeStatusType]] = mapped_column(INTEGER, nullable=True)
    storage_mode_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    storage_connect_status: Mapped[Optional[ConnectStatusType]] = mapped_column(INTEGER, nullable=True)
    storage_connect_status_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    site_der: Mapped["SiteDER"] = relationship(back_populates="site_der_status", lazy="raise", single_parent=True)
    __table_args__ = (UniqueConstraint("site_der_id"),)  # Only one SiteDERSetting allowed per SiteDER)


class SiteLogEvent(Base):
    """Represents a "log event" occurring for a particular site. A log event is any "unusual" event that a site /
    EndDevice has come across through interacting with one of the various function sets."""

    __tablename__ = "site_log_event"

    site_log_event_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("site.site_id", ondelete="CASCADE"))

    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When this record was created

    details: Mapped[Optional[str]] = mapped_column(VARCHAR(32), nullable=True)  # Human readable string
    extended_data: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)  # Additional details from client
    function_set: Mapped[FunctionSetIdentifier] = mapped_column(SMALLINT)  # What function set generated this event
    log_event_code: Mapped[int] = mapped_column(SMALLINT)  # sep2 defined, scoped to the function set
    log_event_id: Mapped[int] = mapped_column(SMALLINT)  # sep2 defined, scoped to the function set
    log_event_pen: Mapped[int] = mapped_column(INTEGER)  # sep2 defined, scoped to the function set
    profile_id: Mapped[ProfileIdentifier] = mapped_column(SMALLINT)

    site: Mapped["Site"] = relationship(lazy="raise")

    __table_args__ = (
        Index("site_log_event_site_id_created_time_log_event_id_idx", "site_id", "created_time", "log_event_id"),
    )


# TODO: deally this would be in the model.doe module. This causes a circular import issue due to the relationship
# mapping between Site and this model. The recommended solution is to use a type.TYPE_CHECKING if statement before the
# imports. However, this causes an issue with the `assertical` testing package that needs to be looked into.
class DefaultSiteControl(Base):
    """Represents fields that map to a subset of the attributes defined in CSIP-AUS' DefaultDERControl resource.
    This entity is linked to a Site."""

    __tablename__ = "default_site_control"
    default_site_control_id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("site.site_id", ondelete="CASCADE"), nullable=False, index=True)

    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When this record was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    import_limit_active_watts: Mapped[Optional[Decimal]] = mapped_column(
        DECIMAL(16, DOE_DECIMAL_PLACES), nullable=True
    )  # Constraint on imported active power
    export_limit_active_watts: Mapped[Optional[Decimal]] = mapped_column(
        DECIMAL(16, DOE_DECIMAL_PLACES), nullable=True
    )  # Constraint on exported active power
    generation_limit_active_watts: Mapped[Optional[Decimal]] = mapped_column(
        DECIMAL(16, DOE_DECIMAL_PLACES), nullable=True
    )
    load_limit_active_watts: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(16, DOE_DECIMAL_PLACES), nullable=True)
    ramp_rate_percent_per_second: Mapped[Optional[int]] = mapped_column(nullable=True)

    site: Mapped["Site"] = relationship(back_populates="default_site_control", lazy="raise")
