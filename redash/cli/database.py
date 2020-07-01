import time

from click import argument, option
from flask.cli import AppGroup
from flask_migrate import stamp
import sqlalchemy
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql import select
from sqlalchemy_utils.types.encrypted.encrypted_type import FernetEngine

from redash.models.base import Column
from redash.models.types import EncryptedConfiguration
from redash.utils.configuration import ConfigurationContainer

manager = AppGroup(help="Manage the database (create/drop tables. reencrypt data.).")


def _wait_for_db_connection(db):
    retried = False
    while not retried:
        try:
            db.engine.execute("SELECT 1;")
            return
        except DatabaseError:
            time.sleep(30)

        retried = True


@manager.command()
def create_tables():
    """Create the database tables."""
    from redash.models import db

    _wait_for_db_connection(db)
    # To create triggers for searchable models, we need to call configure_mappers().
    sqlalchemy.orm.configure_mappers()
    db.create_all()

    # Need to mark current DB as up to date
    stamp()


@manager.command()
def setup_admin():
    from redash.models import Group, Organization, User, db

    _wait_for_db_connection(db)

    org_name = "PlayBlock"
    user_name = "admin"
    email = "admin@playblock.com"
    password = "pbpw"

    default_org = Organization(name=org_name, slug="default", settings={})
    admin_group = Group(
        name="admin",
        permissions=["admin", "super_admin"],
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )
    default_group = Group(
        name="default",
        permissions=Group.DEFAULT_PERMISSIONS,
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )

    db.session.add_all([default_org, admin_group, default_group])
    db.session.commit()

    user = User(
        org=default_org,
        name=user_name,
        email=email,
        group_ids=[admin_group.id, default_group.id],
    )
    user.hash_password(password)

    db.session.add(user)
    db.session.commit()


@manager.command()
def setup_test_admin():
    from redash.models import Group, Organization, User, db

    _wait_for_db_connection(db)

    user_name = "test1"
    email = "test1@playblock.com"
    password = "123456"

    default_org = Organization.get_by_id(1)
    admin_group = Group.get_by_id(1)
    default_group = Group.get_by_id(2)

    user = User(
        org=default_org,
        name=user_name,
        email=email,
        group_ids=[admin_group.id, default_group.id],
    )
    user.hash_password(password)
    db.session.add(user)
    db.session.commit()


@manager.command()
def drop_tables():
    """Drop the database tables."""
    from redash.models import db

    _wait_for_db_connection(db)
    db.drop_all()


@manager.command()
@argument("old_secret")
@argument("new_secret")
@option("--show-sql/--no-show-sql", default=False, help="show sql for debug")
def reencrypt(old_secret, new_secret, show_sql):
    """Reencrypt data encrypted by OLD_SECRET with NEW_SECRET."""
    from redash.models import db

    _wait_for_db_connection(db)

    if show_sql:
        import logging

        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    table_for_select = sqlalchemy.Table(
        "data_sources",
        sqlalchemy.MetaData(),
        Column("id", db.Integer, primary_key=True),
        Column(
            "encrypted_options",
            ConfigurationContainer.as_mutable(
                EncryptedConfiguration(db.Text, old_secret, FernetEngine)
            ),
        ),
    )
    table_for_update = sqlalchemy.Table(
        "data_sources",
        sqlalchemy.MetaData(),
        Column("id", db.Integer, primary_key=True),
        Column(
            "encrypted_options",
            ConfigurationContainer.as_mutable(
                EncryptedConfiguration(db.Text, new_secret, FernetEngine)
            ),
        ),
    )

    update = table_for_update.update()
    data_sources = db.session.execute(select([table_for_select]))
    for ds in data_sources:
        stmt = update.where(table_for_update.c.id == ds["id"]).values(
            encrypted_options=ds["encrypted_options"]
        )
        db.session.execute(stmt)

    data_sources.close()
    db.session.commit()
