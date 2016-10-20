"""A module to to gather metrics about the bootstrap process."""

from collections import OrderedDict
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

from common.config import DB_ENGINE

Base = declarative_base()


class Snapshot(Base):
    __tablename__ = 'snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(String)
    timestamp = Column(DateTime)
    pipeline_status = Column(String)
    validation_status = Column(String)
    nb_validation_errors = Column(Integer)
    resource_type = Column(String)
    extension = Column(String)
    scraper_required = Column(Boolean)
    has_scraper = Column(Boolean)
    country_code = Column(String)
    nuts_code = Column(String)


Base.metadata.create_all(DB_ENGINE)


def get_latest_stats():
    """Get simple stats about the latest update."""

    session = sessionmaker(bind=DB_ENGINE)()

    timestamp = (
        session.query(Snapshot, Snapshot.timestamp)
        .order_by(Snapshot.timestamp.desc())
        .limit(1)
        .all()
        .pop()
    ).timestamp

    stats = session.query(
        Snapshot.pipeline_id,
        Snapshot.pipeline_status,
        Snapshot.resource_type,
        Snapshot.extension,
        Snapshot.validation_status,
    ).filter(Snapshot.timestamp == timestamp)

    # noinspection PyComparisonWithNone
    sum_queries = OrderedDict((
        ('Up', Snapshot.pipeline_status == 'up'),
        ('Remote', Snapshot.resource_type == 'url'),
        ('Local', Snapshot.resource_type == 'path'),
        ('PDF', Snapshot.extension == '.pdf'),
        ('Excel', Snapshot.extension.in_(['.xls', '.xlsx'])),
        ('Broken', Snapshot.validation_status == 'broken'),
        ('Loaded', Snapshot.validation_status == 'loaded'),
        ('Valid', Snapshot.validation_status == 'valid'),
        ('Unknown origin', Snapshot.resource_type == None),  # noqa
        ('Unknown extension', Snapshot.extension == None),  # noqa
    ))

    sums = OrderedDict()
    for key, select in sum_queries.items():
        sums[key] = (
            session.query(Snapshot)
            .filter(and_(Snapshot.timestamp == timestamp, select))
            .count()
        )

    return timestamp, stats, sums
