from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, DateTime, MetaData, Index
from sqlalchemy.sql import and_
from sqlalchemy.sql import select
import datetime
from .settings import *


def get_engine():
    return create_engine("sqlite:///{}processed.db".format(ENV))


def get_processed_table(engine):
    metadata = MetaData()
    processed = Table(
        "processed",
        metadata,
        Column("lab", String),
        Column("filename", String),
        Column("converted_filename", String),
        Column("converted_at", DateTime),
        Column("merged_at", DateTime),
        Index("idx_lab_filename", "lab", "filename", unique=True),
    )
    metadata.create_all(engine)
    return processed


def mark_as_processed(lab, filename, converted_filename):
    engine = get_engine()
    conn = engine.connect()
    ins = get_processed_table(engine).insert()
    conn.execute(
        ins,
        lab=lab,
        filename=filename,
        converted_filename=converted_filename,
        converted_at=datetime.datetime.now(),
    )


def mark_as_merged(lab, converted_filename):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    conn.execute(
        table.update()
        .where(
            and_(table.c.lab == lab, table.c.converted_filename == converted_filename)
        )
        .values(merged_at=datetime.datetime.now())
    )


def get_processed_filenames(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    s = select([table.c.filename]).where(table.c.lab == lab)
    result = conn.execute(s).fetchall()
    return [x[0] for x in result]


def get_unmerged_filenames(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    s = (
        select([table.c.filename, table.c.converted_filename])
        .where(table.c.lab == lab)
        .where(table.c.merged_at == None)
    )
    result = conn.execute(s).fetchall()
    return [(x[0], x[1]) for x in result]


def reset_lab(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    conn.execute(table.delete().where(table.c.lab == lab))
