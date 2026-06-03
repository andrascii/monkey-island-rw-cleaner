import argparse
import dataclasses
import logging
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Iterable
from typing import Optional

import grpc
from sqlalchemy import create_engine
from sqlalchemy import exists
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

import proto.rwmanager_pb2 as proto
import proto.rwmanager_pb2_grpc as proto_grpc
from common.models.db import User
from common.models.db import WataInvoice
from common.models.db import WataTransaction
from common.models.db import YkPayment
from common.models.db import YkRecurrentPayment
from common.setup_logger import setup_logger

logger = logging.getLogger(__name__)


FINAL_YK_STATUSES = {"succeeded", "canceled"}
SUCCESSFUL_WATA_TRANSACTION_STATUSES = {"Paid"}


@dataclasses.dataclass(frozen=True)
class CleanerConfig:
    database_url: str
    rwms_host: str
    rwms_port: int
    grace_days: int = 30
    pending_payment_grace_hours: int = 48
    page_size: int = 500
    max_deletions: int = 25
    execute: bool = False
    include_orphans: bool = False
    username_allowlist: frozenset[str] = frozenset()

    @property
    def cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self.grace_days)

    @property
    def pending_cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(
            hours=self.pending_payment_grace_hours
        )


@dataclasses.dataclass(frozen=True)
class RwmsUserSnapshot:
    uuid: str
    username: str
    status: str
    expire_at: Optional[datetime]
    email: Optional[str] = None
    telegram_id: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class CleanupDecision:
    rwms_user: RwmsUserSnapshot
    should_delete: bool
    reason: str
    local_user_id: Optional[int] = None
    local_expire_at: Optional[datetime] = None


@dataclasses.dataclass
class CleanupSummary:
    scanned: int = 0
    eligible: int = 0
    deleted: int = 0
    skipped: int = 0
    failed: int = 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def protobuf_timestamp_to_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if value.seconds == 0 and value.nanos == 0:
            return None
        return value.ToDatetime().replace(tzinfo=timezone.utc)
    except (AttributeError, ValueError, OverflowError):
        return None


def has_proto_field(message, field_name: str) -> bool:
    try:
        return message.HasField(field_name)
    except ValueError:
        return getattr(message, field_name, None) not in (None, "", 0)


def rwms_user_from_proto(user) -> RwmsUserSnapshot:
    status = (
        proto.UserStatus.Name(user.status) if has_proto_field(user, "status") else ""
    )
    return RwmsUserSnapshot(
        uuid=user.uuid,
        username=user.username,
        status=status,
        expire_at=(
            protobuf_timestamp_to_datetime(user.expire_at)
            if has_proto_field(user, "expire_at")
            else None
        ),
        email=user.email if has_proto_field(user, "email") else None,
        telegram_id=user.telegram_id if has_proto_field(user, "telegram_id") else None,
    )


class RwmsCleanerClient:
    def __init__(self, host: str, port: int):
        options = [("grpc.max_receive_message_length", 300 * 1024 * 1024)]
        self._channel = grpc.insecure_channel(f"{host}:{port}", options=options)
        self._stub = proto_grpc.RwManagerStub(self._channel)

    def close(self):
        self._channel.close()

    def get_all_users(self, offset: int, count: int):
        return self._stub.GetAllUsers(
            proto.GetAllUsersRequest(offset=offset, count=count)
        )

    def get_user_by_uuid(self, uuid: str):
        return self._stub.GetUserByUuid(proto.GetUserByUuidRequest(uuid=uuid))

    def delete_user(self, uuid: str):
        return self._stub.DeleteUser(proto.DeleteUserRequest(uuid=uuid))


def iter_rwms_users(
    client: RwmsCleanerClient, page_size: int
) -> Iterable[RwmsUserSnapshot]:
    offset = 0
    while True:
        reply = client.get_all_users(offset=offset, count=page_size)
        users = list(reply.users)
        if not users:
            break
        for user in users:
            yield rwms_user_from_proto(user)
        offset += len(users)
        if reply.total and offset >= reply.total:
            break


def has_pending_yk_payment(db: Session, user_id: int, pending_cutoff: datetime) -> bool:
    return db.query(
        exists().where(
            (YkPayment.user_id == user_id)
            & (~YkPayment.status.in_(FINAL_YK_STATUSES))
            & (YkPayment.created_at >= pending_cutoff.replace(tzinfo=None))
        )
    ).scalar()


def has_pending_wata_invoice(db: Session, user_id: int, now: datetime) -> bool:
    paid_transaction_exists = (
        db.query(WataTransaction.id)
        .filter(
            WataTransaction.order_id == WataInvoice.order_id,
            WataTransaction.transaction_status.in_(
                SUCCESSFUL_WATA_TRANSACTION_STATUSES
            ),
        )
        .exists()
    )
    return db.query(
        exists().where(
            (WataInvoice.user_id == user_id)
            & (WataInvoice.expiration_datetime >= now)
            & (~paid_transaction_exists)
        )
    ).scalar()


def has_scheduled_recurrent_payment(db: Session, user_id: int) -> bool:
    return db.query(
        exists().where(
            (YkRecurrentPayment.user_id == user_id)
            & (YkRecurrentPayment.scheduled_payment == True)  # noqa: E712
        )
    ).scalar()


def decide_cleanup(
    db: Session,
    rwms_user: RwmsUserSnapshot,
    config: CleanerConfig,
) -> CleanupDecision:
    if (
        config.username_allowlist
        and rwms_user.username not in config.username_allowlist
    ):
        return CleanupDecision(rwms_user, False, "not_in_allowlist")

    if not rwms_user.uuid or not rwms_user.username:
        return CleanupDecision(rwms_user, False, "missing_rwms_identity")

    if rwms_user.expire_at is None:
        return CleanupDecision(rwms_user, False, "rwms_expire_at_missing")

    if rwms_user.expire_at >= config.cutoff:
        return CleanupDecision(rwms_user, False, "rwms_not_old_enough")

    if rwms_user.status == "ACTIVE":
        logger.warning(
            "rwms status mismatch: status=ACTIVE but expire_at=%s is past cutoff "
            "username=%s uuid=%s — skipping deletion",
            rwms_user.expire_at,
            rwms_user.username,
            rwms_user.uuid,
        )
        return CleanupDecision(rwms_user, False, "rwms_status_active_mismatch")

    local_user = db.query(User).filter(User.username == rwms_user.username).first()
    if local_user is None:
        if config.include_orphans:
            return CleanupDecision(rwms_user, True, "orphan_rwms_user")
        return CleanupDecision(rwms_user, False, "local_user_not_found")

    local_expire_at = as_utc(local_user.expire_at)
    if local_expire_at is None:
        return CleanupDecision(
            rwms_user,
            False,
            "local_expire_at_missing",
            local_user_id=local_user.id,
        )

    if local_expire_at >= utc_now():
        return CleanupDecision(
            rwms_user,
            False,
            "local_user_active",
            local_user_id=local_user.id,
            local_expire_at=local_expire_at,
        )

    if local_expire_at >= config.cutoff:
        return CleanupDecision(
            rwms_user,
            False,
            "local_not_old_enough",
            local_user_id=local_user.id,
            local_expire_at=local_expire_at,
        )

    if has_pending_yk_payment(db, local_user.id, config.pending_cutoff):
        return CleanupDecision(
            rwms_user,
            False,
            "pending_yk_payment",
            local_user_id=local_user.id,
            local_expire_at=local_expire_at,
        )

    if has_pending_wata_invoice(db, local_user.id, utc_now()):
        return CleanupDecision(
            rwms_user,
            False,
            "pending_wata_invoice",
            local_user_id=local_user.id,
            local_expire_at=local_expire_at,
        )

    if has_scheduled_recurrent_payment(db, local_user.id):
        return CleanupDecision(
            rwms_user,
            False,
            "scheduled_recurrent_payment",
            local_user_id=local_user.id,
            local_expire_at=local_expire_at,
        )

    return CleanupDecision(
        rwms_user,
        True,
        "expired_in_rwms_and_local_db",
        local_user_id=local_user.id,
        local_expire_at=local_expire_at,
    )


def verify_still_eligible(
    client: RwmsCleanerClient, db: Session, decision: CleanupDecision
) -> bool:
    try:
        fresh_user = rwms_user_from_proto(
            client.get_user_by_uuid(decision.rwms_user.uuid)
        )
    except grpc.RpcError as e:
        logger.error(
            "failed to re-read rwms user before delete uuid=%s username=%s error=%s",
            decision.rwms_user.uuid,
            decision.rwms_user.username,
            e,
        )
        return False

    if fresh_user.username != decision.rwms_user.username:
        logger.error(
            "rwms user changed before delete uuid=%s old_username=%s new_username=%s",
            decision.rwms_user.uuid,
            decision.rwms_user.username,
            fresh_user.username,
        )
        return False

    if (
        fresh_user.expire_at is None
        or fresh_user.expire_at != decision.rwms_user.expire_at
    ):
        logger.error(
            "rwms expire_at changed before delete username=%s old=%s new=%s",
            decision.rwms_user.username,
            decision.rwms_user.expire_at,
            fresh_user.expire_at,
        )
        return False

    if decision.local_user_id is not None:
        fresh_local = db.query(User).filter(User.id == decision.local_user_id).first()
        if fresh_local is None:
            logger.error(
                "local user disappeared before delete user_id=%s username=%s",
                decision.local_user_id,
                decision.rwms_user.username,
            )
            return False
        fresh_expire = as_utc(fresh_local.expire_at)
        if fresh_expire is not None and fresh_expire >= utc_now():
            logger.error(
                "local user became active before delete username=%s user_id=%s expire_at=%s",
                decision.rwms_user.username,
                decision.local_user_id,
                fresh_expire,
            )
            return False

    return True


def run_cleanup(config: CleanerConfig) -> CleanupSummary:
    setup_logger("rw-cleaner.log")
    logger.info(
        "rw cleaner started execute=%s grace_days=%s max_deletions=%s "
        "include_orphans=%s allowlist_size=%s",
        config.execute,
        config.grace_days,
        config.max_deletions,
        config.include_orphans,
        len(config.username_allowlist),
    )

    engine = create_engine(config.database_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine)
    rwms_client = RwmsCleanerClient(config.rwms_host, config.rwms_port)
    summary = CleanupSummary()

    try:
        with session_factory() as db:
            for rwms_user in iter_rwms_users(rwms_client, config.page_size):
                summary.scanned += 1
                decision = decide_cleanup(db, rwms_user, config)
                if not decision.should_delete:
                    summary.skipped += 1
                    logger.info(
                        "skip username=%s uuid=%s reason=%s rwms_expire_at=%s "
                        "local_user_id=%s local_expire_at=%s",
                        rwms_user.username,
                        rwms_user.uuid,
                        decision.reason,
                        rwms_user.expire_at,
                        decision.local_user_id,
                        decision.local_expire_at,
                    )
                    continue

                summary.eligible += 1
                if summary.deleted >= config.max_deletions:
                    summary.skipped += 1
                    logger.warning(
                        "skip username=%s uuid=%s reason=max_deletions_reached",
                        rwms_user.username,
                        rwms_user.uuid,
                    )
                    continue

                if not config.execute:
                    logger.warning(
                        "dry-run delete candidate username=%s uuid=%s reason=%s "
                        "rwms_expire_at=%s local_user_id=%s local_expire_at=%s",
                        rwms_user.username,
                        rwms_user.uuid,
                        decision.reason,
                        rwms_user.expire_at,
                        decision.local_user_id,
                        decision.local_expire_at,
                    )
                    continue

                if not verify_still_eligible(rwms_client, db, decision):
                    summary.failed += 1
                    continue

                try:
                    response = rwms_client.delete_user(rwms_user.uuid)
                except grpc.RpcError as e:
                    summary.failed += 1
                    logger.error(
                        "delete failed username=%s uuid=%s error=%s",
                        rwms_user.username,
                        rwms_user.uuid,
                        e,
                    )
                    continue

                if response.is_deleted:
                    summary.deleted += 1
                    logger.warning(
                        "deleted username=%s uuid=%s rwms_expire_at=%s "
                        "local_user_id=%s local_expire_at=%s",
                        rwms_user.username,
                        rwms_user.uuid,
                        rwms_user.expire_at,
                        decision.local_user_id,
                        decision.local_expire_at,
                    )
                else:
                    summary.failed += 1
                    logger.error(
                        "delete returned false username=%s uuid=%s",
                        rwms_user.username,
                        rwms_user.uuid,
                    )
    finally:
        rwms_client.close()
        engine.dispose()

    logger.info(
        "rw cleaner finished scanned=%s eligible=%s deleted=%s skipped=%s failed=%s",
        summary.scanned,
        summary.eligible,
        summary.deleted,
        summary.skipped,
        summary.failed,
    )
    return summary


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "да", "вкл"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    return int(value)


def split_csv(value: Optional[str]) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(item.strip() for item in value.split(",") if item.strip())


def parse_args(argv: Optional[list[str]] = None) -> CleanerConfig:
    parser = argparse.ArgumentParser(
        description="Safely delete old expired RWMS subscriptions."
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--rwms-host", default=os.getenv("RWMS_HOST"))
    parser.add_argument("--rwms-port", type=int, default=env_int("RWMS_PORT", 0))
    parser.add_argument("--grace-days", type=int, default=env_int("GRACE_DAYS", 30))
    parser.add_argument(
        "--pending-payment-grace-hours",
        type=int,
        default=env_int("PENDING_PAYMENT_GRACE_HOURS", 48),
    )
    parser.add_argument("--page-size", type=int, default=env_int("PAGE_SIZE", 500))
    parser.add_argument(
        "--max-deletions", type=int, default=env_int("MAX_DELETIONS", 25)
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=env_bool("EXECUTE", False),
        help="Actually delete users from RWMS. Default is dry-run.",
    )
    parser.add_argument(
        "--include-orphans",
        action="store_true",
        default=env_bool("INCLUDE_ORPHANS", False),
        help="Allow deleting RWMS users that have no local users row.",
    )
    parser.add_argument(
        "--username-allowlist",
        default=os.getenv("USERNAME_ALLOWLIST", ""),
        help="Comma-separated usernames to consider. Empty means all users.",
    )
    args = parser.parse_args(argv)

    missing = [
        name
        for name, value in {
            "DATABASE_URL": args.database_url,
            "RWMS_HOST": args.rwms_host,
            "RWMS_PORT": args.rwms_port,
        }.items()
        if not value
    ]
    if missing:
        parser.error(f"Missing required settings: {', '.join(missing)}")

    if args.grace_days < 30:
        parser.error("--grace-days must be at least 30")
    if args.max_deletions < 1:
        parser.error("--max-deletions must be positive")
    if args.page_size < 1:
        parser.error("--page-size must be positive")

    return CleanerConfig(
        database_url=args.database_url,
        rwms_host=args.rwms_host,
        rwms_port=args.rwms_port,
        grace_days=args.grace_days,
        pending_payment_grace_hours=args.pending_payment_grace_hours,
        page_size=args.page_size,
        max_deletions=args.max_deletions,
        execute=args.execute,
        include_orphans=args.include_orphans,
        username_allowlist=split_csv(args.username_allowlist),
    )


def main(argv: Optional[list[str]] = None) -> int:
    config = parse_args(argv)
    summary = run_cleanup(config)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
