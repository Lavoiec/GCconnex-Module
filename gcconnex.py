import sqlalchemy as sq


from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from sqlalchemy import text
from sqlalchemy.orm import aliased
from sqlalchemy import or_


import pandas as pd


import datetime as dt
import getpass

# TURN CONNECTION AND INITIALIZTION INTO CLASSES AND STUFF... CLASS


def convert_unixtime(stamp):

    return dt.datetime.fromtimestamp(
        int(stamp)
    ).strftime('%Y-%m-%d')


def convert_if_time(y):
    """
    Used for applying on a dataframe

    If the column name contains 'id', then applies the fromtimestamp() and strftime() sequence

    thanks to a windows error, the if condition makes it so that corrupted time entries are recorded to 2008
    """
    if 'id' not in y.name and (y.dtype == 'int64') and ('subtype' not in y.name):

        y = y.apply(lambda x: dt.datetime.fromtimestamp(x).strftime('%Y-%m-%d') if x >86399 else dt.datetime.fromtimestamp(86400))
        return y
    else:
        return y


def connect_to_database():
    global engine
    global conn
    """
    One of two essential commands to access database
    Need credentials to run.
    """
# Need to manually enter the username
# password, and the database to create a valid connection
    username = getpass.getpass("Username")
    password = getpass.getpass("Password")
    database = getpass.getpass("Database")

    db_connection = "mysql+pymysql://{}:{}@192.168.1.99:3306/{}".format(
        username, password, database)

    engine = sq.create_engine(db_connection, encoding='latin1', echo=False)

    conn = engine.connect()

    engine.connect()

    return engine, conn

    # Creates the link to GCconnex database


def create_session():
    """
    Second part of the essential commands
    """

    global session
    global Base

    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    # Maps the relationships
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    return session, Base


class users(object):
    """
     The connection to the Users table.
    """
    def get_all():  # Grabs entire table
        """
        Queries the entire users table
        """
        users_table = Base.classes.elggusers_entity
        user_query = session.query(users_table).statement

        users = pd.read_sql(user_query, conn)

        users = users.apply(convert_if_time)
        return users

    def filter_(filter_condition):
        """
        Allows you to enter a filter in the query for the entire users table
        Pass it as a string into the function.

        PROTIP: Use single quotes to filter out using strings

        ex.

        gc.users.filter_("name = 'USER_NAME'")
        """
        users_session = session.query(users_table)
        users = pd.read_sql(
            users_session.filter(
                text("{}".format(filter_condition))
            ).statement, conn
        )

        return users

    def department():
        """
        Returns:
        [guid, name, email, last_action, prev_last_action, last_login, prev_last_login, time_created, department]

        Note: This only returns entries that have filled out their department entry.
        This returns a partial list of users.
        """

        users_table = Base.classes.elggusers_entity
        entities_table = Base.classes.elggentities
        metadata_table = Base.classes.elggmetadata
        metastrings_table = Base.classes.elggmetastrings

        statement = session.query(
            users_table.guid,
            users_table.name,
            users_table.email,
            users_table.last_action,
            users_table.prev_last_action,
            users_table.last_login,
            users_table.prev_last_login,
            entities_table.time_created,
            metastrings_table.string
        )

        statement = statement.filter(metastrings_table.id == metadata_table.value_id)
        statement = statement.filter(metadata_table.name_id == 8667)
        statement = statement.filter(metadata_table.entity_guid == users_table.guid)
        statement = statement.filter(entities_table.guid == users_table.guid)
        statement = statement.statement

        users_department = pd.read_sql(statement, conn)

        users_department =users_department.apply(convert_if_time)

        return users_department


class groups(object):
    """
    Returns groups: The principle object in GCconnex
    """

    def get_all(tags=False):
        """
        If tags is false, only returns groups table
        returns [guid, title, description]

        if tags is true, it merges with
        the metadata and metastrings table,
        returning all of both the groups table, metadata table
        and the tag string
        """

        if tags is False:

            groups_table = Base.classes.elgggroups_entity
            groups_query = session.query(groups_table).statement
            get_it_all = pd.read_sql(groups_query, conn)

            get_it_all = get_it_all.apply(convert_if_time)

            return get_it_all

        elif tags is True:

            groups_table = Base.classes.elgggroups_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                groups_table,
                metadata_table,
                metastrings_table.string
            )

            # The tags features for groups are kept in the database as "interests"
            statement = statement.filter(groups_table.guid == metadata_table.entity_guid)
            statement = statement.filter(metadata_table.name_id == 59)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            get_it_all = pd.read_sql(statement, conn)
            get_it_all.drop(['id', 'value_id'], axis=1, inplace=True)

            tags = get_it_all[['name', 'string']]

            # Gathers all the tags that are associated with a group
            # and brings them into one list that is stored in the dataframe
            # under a column
            get_it_all = (pd.DataFrame(
                tags
                .groupby('name')['string']
                .apply(list))
                .reset_index()
                .merge(get_it_all.drop('string', axis=1)
                .drop_duplicates(), on='name')
                )

            get_it_all = get_it_all.apply(convert_if_time)
            return get_it_all

    def filter_(filter_condition):  # Allows for flexible SQL filters
        groups_table = Base.classes.elgggroups_entity
        groups_session = session.query(groups_table)

        groups_ = pd.read_sql(
            groups_session
            .filter(text("{}".format(filter_condition)))
            .statement, conn
        )
        groups_ = groups_.apply(convert_if_time)
        return groups_

    def get_membership(department=False):
        """
        Returns members of groups.
        returns [user_name, user_guid, group_name, group_guid, time_of_join, user_department]

        Contains same caveat as users by department. Only returns members of a group that
        have indicated their department
        """

        users_table = Base.classes.elggusers_entity
        groups_table = Base.classes.elgggroups_entity
        metadata_table = Base.classes.elggmetadata
        metastrings_table = Base.classes.elggmetastrings
        relationships_table = Base.classes.elggentity_relationships

        if department is True:

            statement = session.query(
                users_table.name,
                users_table.guid,
                groups_table.name,
                groups_table.guid,
                relationships_table.time_created,
                metastrings_table.string
            )

            statement = statement.filter(users_table.guid == relationships_table.guid_one)
            statement = statement.filter(groups_table.guid == relationships_table.guid_two)

            statement = statement.filter(relationships_table.relationship == 'member')
            statement = statement.filter(metastrings_table.id == metadata_table.value_id)
            statement = statement.filter(metadata_table.name_id == 8667)
            statement = statement.filter(metadata_table.entity_guid == users_table.guid)

            statement = statement.statement

            get_all = pd.read_sql(statement, conn)

            get_all.columns = [
                'user_name',
                'user_guid',
                'group_name',
                'group_guid',
                'time_created',
                'department'
            ]

            get_all = get_all.apply(convert_if_time)

            return get_all

        else:

            statement = session.query(
                users_table.name,
                users_table.guid,
                groups_table.name,
                groups_table.guid,
                relationships_table.time_created
            )

            statement = statement.filter(users_table.guid == relationships_table.guid_one)
            statement = statement.filter(groups_table.guid == relationships_table.guid_two)

            statement = statement.filter(relationships_table.relationship == 'member')

            statement = statement.statement

            get_all = pd.read_sql(statement, conn)

            get_all.columns = [
                'user_name',
                'user_guid',
                'group_name',
                'group_guid',
                'time_created'
            ]

            get_all = get_all.apply(convert_if_time)
            return get_all

    def get_group_sizes():
        """
        Returns the group guid, group name, and the number of users in the group
        """
        groups_table = Base.classes.elgggroups_entity
        relationships_table = Base.classes.elggentity_relationships
        statement = session.query(
            groups_table.guid,
            groups_table.name,
            relationships_table.guid_one
        )

        statement = statement.filter(
            groups_table.guid == relationships_table.guid_two
        )

        statement = statement.filter(
            relationships_table.relationship == 'member'
        )

        statement = statement.statement

        get_groups_sizes = pd.read_sql(statement, conn).groupby("name").count()

        get_groups_sizes = get_groups_sizes.apply(convert_if_time)

        return get_groups_sizes


class entities(object):
    """
    Access to the entities table: It contains
    information on every object in the GCconnex instance
    """
    def get_all():

        entities_table = Base.classes.elggentities
        entities_query = session.query(entities_table).statement

        get_it_all = pd.read_sql(entities_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        entities_table = Base.classes.elggentities

        entities_session = session.query(entities_table)

        entities_ = pd.read_sql(
            entities_session.filter(
                text("{}".format(filter_condition))
            ).statement, conn
        )

        entities_ = entities_.apply(convert_if_time)

        return entities_


class metadata(object):
    """
    Contains data on the entities in GCconnex
    """
    def get_all():
        metadata_table = Base.classes.elggmetadata
        metadata_query = session.query(metadata_table).statement
        get_it_all = pd.read_sql(metadata_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        metadata_table = Base.classes.elggmetadata
        metadata_session = session.query(metadata_table)

        metadatas = pd.read_sql(
            metadata_session
            .filter(text("{}".format(filter_condition)))
            .statement, conn
        )

        metadatas = metadatas.apply(convert_if_time)

        return metadatas


class metastrings(object):
    """
    Contains the readable strings linked to the metadata table
    """
    def get_all():

        metastrings_table = Base.classes.elggmetastrings

        metastrings_query = session.query(metastrings_table).statement

        get_it_all = pd.read_sql(metastrings_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        metastrings_table = Base.classes.elggmetastrings

        metastrings_session = session.query(metastrings_table)

        metastring = pd.read_sql(metastrings_session
                .filter(text("{}".format(filter_condition)))
                .statement, conn
        )

        metastring = metastring.apply(convert_if_time)

        return metastring


class relationships(object):
    """
    The table that logs all interactions between entities in the database.
    """
    def get_all():

        relationships_table = Base.classes.elggentity_relationships

        relationships_query = session.query(relationships_table).statement

        get_it_all = pd.read_sql(relationships_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        relationships_table = Base.classes.elggentity_relationships
        relationships_session = session.query(relationships_table)

        relationship = pd.read_sql(
            relationships_session
            .filter(text("{}".format(filter_condition)))
            .statement, conn
        )

        relationship = relationship.apply(convert_if_time)

        return relationship


class annotations(object):

    def get_all():

        annotations_table = Base.classes.elggannotations

        annotations_query = session.query(annotations_table).statement

        get_it_all = pd.read_sql(annotations_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        annotations_table = Base.classes.elggannotations

        annotations_session = session.query(annotations_table)

        annotation = pd.read_sql(
            annotations_session
            .filter(text("{}".format(filter_condition)))
            .statement, conn
        )

        annotation = annotation.apply(convert_if_time)

        return annotation


class objectsentity(object):

    def get_all():

        objectsentity_table = Base.classes.elggobjects_entity

        objectsentity_query = session.query(objectsentity_table).statement

        get_it_all = pd.read_sql(objectsentity_query, conn)

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def filter_(filter_condition):

        objectsentity_table = Base.classes.elggobjects_entity

        objectsentity_session = session.query(objectsentity_table)

        elggobject = pd.read_sql(
            objectsentity_session
            .filter(text("{}".format(filter_condition)))
            .statement, conn
        )

        elggobject = elggobject.apply(convert_if_time)

        return elggobject


class micromissions(object):

    """
    Special class that deals directly with Job Opportunities:
    A popular feature on GCconnex
    """

    def get_users():
        """
        Returns the users who have opted into the micromissions
        feature of GCconnex
        """
        users_table = Base.classes.elggusers_entity
        metadata_table = Base.classes.elggmetadata
        metastrings_table = Base.classes.elggmetastrings

        metastrings_table_2 = aliased(metastrings_table)
        metadata_table_2 = aliased(metadata_table)

        statement = session.query(
            users_table.guid,
            users_table.name,
            users_table.email,
            metadata_table.name_id,
            metadata_table.value_id,
            metadata_table_2.value_id,
            metastrings_table,
            metastrings_table_2
        )

        statement = statement.filter(users_table.guid == metadata_table.entity_guid)
        statement = statement.filter(metadata_table_2.entity_guid == users_table.guid)
        statement = statement.filter(metastrings_table.id == metadata_table.value_id)
        statement = statement.filter(metastrings_table_2.id == metadata_table_2.value_id)
        statement = statement.filter(metadata_table_2.name_id == 8667)
        statement = statement.filter(metadata_table.name_id == 1192767)

        statement = statement.statement
        get_it_all = pd.read_sql(statement, conn)
        get_it_all.columns = [
            'guid',
            'name',
            'email',
            'md1_name_id',
            'md1_value_id',
            'md2_value_id',
            'ms1_id',
            'opt-in',
            'ms2_id',
            'department'
        ]

        get_it_all = get_it_all.apply(convert_if_time)

        return get_it_all

    def get_aggregate():
        """
        Returns a count of users who have opted into micromissions
        and users who have not
        """

        metadata_table = Base.classes.elggmetadata
        metastrings_table = Base.classes.elggmetastrings

        statement = session.query(metastrings_table.string, metadata_table.entity_guid)
        statement = statement.filter(metastrings_table.id == metadata_table.value_id)

        statement = statement.filter(metadata_table.name_id == 1192767)

        statement = statement.statement

        get_it_all = pd.read_sql(statement, conn).groupby("string").count()
        get_it_all.columns = ['Count']
        return get_it_all

    def get_mission_data(summary=False):  
        # This needs to use the text query
        # Since SQLAlchemy does not suppport IN statements well

        """
        returns
        [mission_guid, mission_title, mission_action, mission_state, mission_type, time_of_relationship ]

        If summary is true, returns a summary table to the different mission types
        """
        mission_data_string = """SELECT oe.guid guid,
         oe.title title,
         r.relationship relationship,
         ms.string state,
         ms2.string type,
         r.time_created time_of_relationship
        FROM elggobjects_entity oe,
        elggentity_relationships r,
        elggmetadata md, elggmetastrings ms,
        elggmetadata md2, elggmetastrings ms2
        WHERE oe.guid = r.guid_one
        AND r.guid_one IN (SELECT guid FROM elggentities WHERE subtype = 70)
        AND md.entity_guid = oe.guid
        AND ms.id = md.value_id
        AND md2.entity_guid = oe.guid
        AND md2.name_id = 1209635
        AND ms2.id = md2.value_id
        AND md.name_id = 126
        ORDER BY r.time_created"""

        get_data = pd.read_sql(mission_data_string, conn)
        get_data = get_data.apply(convert_if_time)

        if summary is True:
            get_data = pd.crosstab(
                get_data.type, get_data.relationship)

        return get_data


class content(object):
    """
    Special content class

    Returns the type of content as stated in the method name.
    """

    def get_blogs(tags=False):

        if tags is False:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity

            statement = session.query(entities_table, objectsentity_table)

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 5)

            statement = statement.statement

            get_blogs = pd.read_sql(statement, conn)

            get_blogs = get_blogs.apply(convert_if_time)
            return get_blogs

        elif tags is True:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                entities_table.guid,
                entities_table.time_created,
                entities_table.container_guid,
                objectsentity_table.title,
                objectsentity_table.description,
                metastrings_table.string
            )

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 5)
            statement = statement.filter(metadata_table.name_id == 119)
            statement = statement.filter(metadata_table.entity_guid == entities_table.guid)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            gdt = pd.read_sql(statement, conn)

            tags = gdt[['title', 'string']]

            get_blogs = pd.DataFrame(tags.groupby('title')['string'].apply(list)).reset_index().merge(gdt.drop('string', axis = 1).drop_duplicates(), on = 'title')

            get_blogs = get_blogs.apply(convert_if_time)

            return get_blogs

    def get_discussions(tags=False):

        if tags is False:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity

            statement = session.query(entities_table, objectsentity_table)

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 7)

            statement = statement.statement

            get_discussions = pd.read_sql(statement, conn)

            get_discussions = get_discussions.apply(convert_if_time)

            return get_discussions

        elif tags is True:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                entities_table.guid,
                entities_table.time_created,
                entities_table.container_guid,
                objectsentity_table.title,
                objectsentity_table.description, metastrings_table.string
            )

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 7)
            statement = statement.filter(metadata_table.name_id == 119)
            statement = statement.filter(metadata_table.entity_guid == entities_table.guid)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            gdt = pd.read_sql(statement, conn)

            tags = gdt[['title', 'string']]

            get_discussions = pd.DataFrame(tags.groupby('title')['string'].apply(list)).reset_index().merge(gdt.drop('string', axis = 1).drop_duplicates(), on = 'title')

            get_discussions = get_discussions.apply(convert_if_time)

            return get_discussions

    def get_files(tags=False):

        if tags is False:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity

            statement = session.query(entities_table, objectsentity_table)

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 1)

            statement = statement.statement

            get_files = pd.read_sql(statement, conn)

            get_files = get_files.apply(convert_if_time)

            return get_files

        elif tags is True:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                entities_table.guid,
                entities_table.time_created,
                entities_table.container_guid,
                objectsentity_table.title,
                objectsentity_table.description,
                metastrings_table.string
            )

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 1)
            statement = statement.filter(metadata_table.name_id == 119)
            statement = statement.filter(metadata_table.entity_guid == entities_table.guid)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            gdt = pd.read_sql(statement, conn)

            tags = gdt[['title', 'string']]

            get_files = pd.DataFrame(tags.groupby('title')['string'].apply(list)).reset_index().merge(gdt.drop('string', axis = 1).drop_duplicates(), on = 'title')

            get_files = get_files.apply(convert_if_time)
            return get_files

    def get_bookmarks(tags=False):

        if tags is False:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity

            statement = session.query(entities_table, objectsentity_table)

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 8)

            statement = statement.statement

            get_bookmarks = pd.read_sql(statement, conn)

            get_bookmarks = get_bookmarks.apply(convert_if_time)

            return get_bookmarks

        elif tags is True:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                entities_table.guid,
                entities_table.time_created,
                entities_table.container_guid,
                objectsentity_table.title,
                objectsentity_table.description,
                metastrings_table.string
            )

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 8)
            statement = statement.filter(metadata_table.name_id == 119)
            statement = statement.filter(metadata_table.entity_guid == entities_table.guid)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            gdt = pd.read_sql(statement, conn)

            tags = gdt[['title', 'string']]

            get_bookmarks = pd.DataFrame(tags.groupby('title')['string'].apply(list)).reset_index().merge(gdt.drop('string', axis = 1).drop_duplicates(), on = 'title')
            get_bookmarks = get_bookmarks.apply(convert_if_time)

            return get_bookmarks

    def get_ideas(tags=False):

        if tags is False:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity

            statement = session.query(entities_table, objectsentity_table)

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 42)
            statement = statement.statement
            get_ideas = pd.read_sql(statement, conn)
            get_ideas = get_ideas.apply(convert_if_time)

            return get_ideas

        elif tags is True:

            entities_table = Base.classes.elggentities
            objectsentity_table = Base.classes.elggobjects_entity
            metadata_table = Base.classes.elggmetadata
            metastrings_table = Base.classes.elggmetastrings

            statement = session.query(
                entities_table.guid,
                entities_table.time_created,
                entities_table.container_guid,
                objectsentity_table.title,
                objectsentity_table.description,
                metastrings_table.string
            )

            statement = statement.filter(entities_table.guid == objectsentity_table.guid)
            statement = statement.filter(entities_table.subtype == 42)
            statement = statement.filter(metadata_table.name_id == 119)
            statement = statement.filter(metadata_table.entity_guid == entities_table.guid)
            statement = statement.filter(metadata_table.value_id == metastrings_table.id)

            statement = statement.statement

            gdt = pd.read_sql(statement, conn)

            tags = gdt[['title', 'string']]

            get_ideas = pd.DataFrame(tags.groupby('title')['string'].apply(list)).reset_index().merge(gdt.drop('string', axis = 1).drop_duplicates(), on = 'title')
            get_ideas = get_ideas.apply(convert_if_time)
            return get_ideas

    def get_comments():
        entities_table = Base.classes.elggentities
        objectsentity_table = Base.classes.elggobjects_entity

        statement = session.query(
            entities_table.guid,
            entities_table.owner_guid,
            entities_table.time_created,
            entities_table.container_guid,
            entities_table.subtype,
            objectsentity_table.title,
            objectsentity_table.description
        )
        statement = statement.filter(
            entities_table.guid == objectsentity_table.guid)

        statement = statement.filter(or_(
            entities_table.subtype == 66, entities_table.subtype == 64))

        statement = statement.statement

        comments = pd.read_sql(statement, conn)

        return comments

class communities(object):

    def get_content_community(summary=False, groupby_vals=['string', 'subtype']):

        entities_table = Base.classes.elggentities
        metadata_table = Base.classes.elggmetadata
        metastrings_table = Base.classes.elggmetastrings

        statement = session.query(
                entities_table.guid,
                entities_table.subtype,
                entities_table.time_created,
                metadata_table.value_id,
                metastrings_table.string    
        )

        statement = statement.filter(
                metadata_table.entity_guid == entities_table.guid,
                metastrings_table.id == metadata_table.value_id,
                metadata_table.name_id == 35557,
                entities_table.subtype != 67,
                entities_table.subtype != 4
        )

        statement = statement.statement

        content_community = pd.read_sql(statement, conn)

        if summary:
            content_community = content_community.groupby(groupby_vals).count()['guid']

        return content_community
