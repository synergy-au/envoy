from datetime import datetime
from enum import IntEnum, auto
from typing import Optional

from envoy_schema.server.schema.sep2.pub_sub import ConditionAttributeIdentifier
from sqlalchemy import INTEGER, VARCHAR, DateTime, ForeignKey, Index, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from envoy.server.model import Base, Site
from envoy.server.model.aggregator import Aggregator


class SubscriptionResource(IntEnum):
    """The different types of resources that can be subscribed to"""

    SITE = auto()
    DYNAMIC_OPERATING_ENVELOPE = auto()
    TARIFF_GENERATED_RATE = auto()
    READING = auto()
    SITE_DER_AVAILABILITY = auto()
    SITE_DER_RATING = auto()
    SITE_DER_SETTING = auto()
    SITE_DER_STATUS = auto()
    DEFAULT_SITE_CONTROL = auto()
    FUNCTION_SET_ASSIGNMENTS = auto()  # This just maps to the pollRate exposed in the FSA List
    SITE_CONTROL_GROUP = auto()


class Subscription(Base):
    """A subscription represents a remote client wanting to receive webhook notifications about resource(s)
    as they change. Subscriptions are scoped to a particular resource or list of resources and optionally
    specific sites."""

    __tablename__ = "subscription"

    subscription_id: Mapped[int] = mapped_column(primary_key=True)
    aggregator_id: Mapped[int] = mapped_column(ForeignKey("aggregator.aggregator_id"))  # Aggregator that owns this sub
    created_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # When the subscription was created
    changed_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # When the subscription was last altered

    resource_type: Mapped[SubscriptionResource] = mapped_column(INTEGER)  # What resource type is being subscribed to
    resource_id: Mapped[Optional[int]] = mapped_column(
        INTEGER, nullable=True
    )  # Represents the ID of a single resource being subscribed or if NULL, the list of all available resources
    scoped_site_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("site.site_id"), nullable=True
    )  # If set - this subscription is scoped to this specific site_id

    notification_uri: Mapped[str] = mapped_column(VARCHAR(length=2048))  # remote URI where notifications will be sent
    entity_limit: Mapped[int] = mapped_column(INTEGER)  # The max number of entities to return in a single notification

    aggregator: Mapped[Aggregator] = relationship(lazy="raise")
    scoped_site: Mapped[Optional[Site]] = relationship(lazy="raise")
    conditions: Mapped[list["SubscriptionCondition"]] = relationship(
        back_populates="subscription",
        lazy="raise",
        cascade="all, delete",
        passive_deletes=True,
    )  # The set of conditions that might limit the firing of a notification

    __table_args__ = (
        Index("ix_subscription_aggregator_id_resource_type", "aggregator_id", "resource_type", unique=False),
    )


class SubscriptionCondition(Base):
    """Condition on a subscription that might limit whether it fires or not based on a particular attribute
    in the resource being subscribed to. Eg - Only fire on Reading if Voltage is > X and < Y.

    If a Subscription has multiple conditions then they must ALL be met in order for a notification to be raised"""

    __tablename__ = "subscription_condition"

    subscription_condition_id: Mapped[int] = mapped_column(primary_key=True)
    subscription_id: Mapped[int] = mapped_column(ForeignKey("subscription.subscription_id", ondelete="CASCADE"))

    attribute: Mapped[ConditionAttributeIdentifier] = mapped_column(INTEGER)
    lower_threshold: Mapped[int] = mapped_column(
        INTEGER, nullable=False
    )  # If set - min of attribute value required to fire notification
    upper_threshold: Mapped[int] = mapped_column(
        INTEGER, nullable=False
    )  # If set - max of attribute value required to fire notification

    subscription: Mapped["Subscription"] = relationship(back_populates="conditions", lazy="raise")


class TransmitNotificationLog(Base):
    """Represents a single attempt to transmit a subscription notification to a remote source. This will be a heavily
    used table so its been optimised for storage size."""

    __tablename__ = "transmit_notification_log"

    transmit_notification_log_id: Mapped[int] = mapped_column(primary_key=True)
    subscription_id_snapshot: Mapped[int] = (
        mapped_column()
    )  # The moment in time snapshot of the source subscription's ID. For maximum accuracy, the archive_subscription
    # should be consulted for values around transmit_time
    transmit_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # When did transmission start
    transmit_duration_ms: Mapped[int] = mapped_column(INTEGER)  # How long did transmission take (in milli seconds)
    notification_size_bytes: Mapped[int] = mapped_column(INTEGER)  # Size of notification content body (in bytes)
    attempt: Mapped[int] = mapped_column(INTEGER)  # What was the attempt number being transmitted
    http_status_code: Mapped[int] = mapped_column(INTEGER)  # HTTP Status received (or -1 on other error)
