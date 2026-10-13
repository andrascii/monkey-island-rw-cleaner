"""Фикстуры тестов rw-cleaner: SQLite в памяти с полной схемой
(Base.metadata.create_all, PRAGMA foreign_keys=ON)."""

import itertools

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import tests.helpers  # noqa: F401 - регистрирует JSONB→JSON для SQLite
from common.models.db import Base
from common.models.db import SubscriptionArchive


@pytest.fixture
def engine():
    engine = create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine, "connect")
    def _fk_on(dbapi_connection, _record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def archive_ids():
    """BIGINT IDENTITY на SQLite не автоинкрементится — id архива задаём
    сами (в PostgreSQL его выдаёт IDENTITY)."""
    counter = itertools.count(1000)

    def _assign(_mapper, _connection, target):
        if target.id is None:
            target.id = next(counter)

    event.listen(SubscriptionArchive, "before_insert", _assign)
    yield
    event.remove(SubscriptionArchive, "before_insert", _assign)
