import sqlalchemy as sq


from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import and_, or_
from sqlalchemy import text
from sqlalchemy.orm import aliased


import pandas as pd
import numpy as np

import os
import csv

import datetime as dt
import getpass	





	
def connect_to_database():
	global engine
	global conn



# Need to manually enter the username, password, and the database to create a valid connection
	username = getpass.getpass("Username")
	password = getpass.getpass("Password")
	database = getpass.getpass("Database")

	db_connection = "mysql+pymysql://{}:{}@192.168.1.99:3306/{}".format(
		username, password, database)

	engine = sq.create_engine(db_connection,encoding='latin1', echo=False)

	conn = engine.connect()

	engine.connect()
	

	return engine, conn	


		# Creates the link to GCconnex database

def create_session():

	global session
	global Base

	Session = sessionmaker(bind = engine)
	Session.configure(bind = engine)
	session = Session()

	# Maps the relationships
	Base = automap_base()
	Base.prepare(engine, reflect = True)

	return session, Base


class users(object): # Pulls in the entire users database
	
	
	
	def getall(): # Grabs entire table


		users_table = Base.classes.elggusers_entity
		user_query =  session.query(users_table).statement
		
		users = pd.read_sql(user_query, conn)
		
		return users
		
	def filter_(filter_condition): # Merge between pandas, SQL and SQLalchemy syntax
		users_session = session.query(users_table)
		users = pd.read_sql(users_session.filter(text("{}".format(filter_condition))).statement, conn) # Needs many quotes
		
		return users # If you want to query on a text statement, need "" around the filter_condition then '' around the 
					 # string you wish to filter on
   

	def department(): # Issue : doesn't pull all members. That's bad.

		users_table = Base.classes.elggusers_entity
		entities_table = Base.classes.elggentities
		metadata_table = Base.classes.elggmetadata
		metastrings_table = Base.classes.elggmetastrings
		
		statement = session.query(users_table, entities_table.time_created, metastrings_table.string, )
		statement = statement.filter(metastrings_table.id == metadata_table.value_id)
		statement = statement.filter(metadata_table.name_id == 8667)
		statement = statement.filter(metadata_table.entity_guid == users_table.guid)
		statement = statement.filter(entities_table.guid == users_table.guid)
		statement = statement.statement
		
		users_department = pd.read_sql(statement, conn)
		
		return users_department
		
		


class groups(object):
	

	
	def getall():
		groups_table = Base.classes.elgggroups_entity
		groups_query = session.query(groups_table).statement
		get_it_all = pd.read_sql(groups_query, conn)
		
		return get_it_all
	
	
	def filter_(filter_condition): # Allows for flexible SQL filters (any where statement)
		groups_table = Base.classes.elgggroups_entity
		groups_session = session.query(groups_table)
		groups_ = pd.read_sql(groups_session.filter(text("{}".format(filter_condition))).statement, conn) # Needs many quotes
		
		return groups_ # If you want to query on a text statement, need "" around the filter_condition then '' around the 
					 # string you wish to filter on

	def get_membership(department = False): # Takes elggentity_relationships, elggusers_entity, elgggroups_entity, elggentities
						   # ue.name, ue.guid, ge.name, ge.guid, ge.name, e.time_created

		users_table = Base.classes.elggusers_entity
		groups_table = Base.classes.elgggroups_entity
		metadata_table = Base.classes.elggmetadata
		metastrings_table = Base.classes.elggmetastrings
		relationships_table = Base.classes.elggentity_relationships
		

		if department == True:
		
			statement = session.query(users_table.name, users_table.guid, groups_table.name, groups_table.guid,
								  relationships_table.time_created, metastrings_table.string)
		
			statement = statement.filter(users_table.guid == relationships_table.guid_one)
			statement = statement.filter(groups_table.guid == relationships_table.guid_two)

			statement = statement.filter(relationships_table.relationship == 'member')
			statement = statement.filter(metastrings_table.id == metadata_table.value_id)
			statement = statement.filter(metadata_table.name_id == 8667)
			statement = statement.filter(metadata_table.entity_guid == users_table.guid)
		
			statement = statement.statement
		
			get_all = pd.read_sql(statement, conn)
		
			get_all.columns = ['user_name', 'user_guid', 'group_name', 'group_guid', 'time_created', 'department']
		
			return get_all
		
		else:
			
			statement = session.query(users_table.name, users_table.guid, groups_table.name, groups_table.guid,
								  relationships_table.time_created)
		
			statement = statement.filter(users_table.guid == relationships_table.guid_one)
			statement = statement.filter(groups_table.guid == relationships_table.guid_two)

			statement = statement.filter(relationships_table.relationship == 'member')
			
			statement = statement.statement
		
			get_all = pd.read_sql(statement, conn)
		
			get_all.columns = ['user_name', 'user_guid', 'group_name', 'group_guid', 'time_created']
		
			return get_all
		
	
	
  



	def get_group_sizes():



		groups_table = Base.classes.elgggroups_entity
		relationships_table = Base.classes.elggentity_relationships
		
		statement = session.query(groups_table.name, relationships_table.guid_one)
		
		statement = statement.filter(groups_table.guid == relationships_table.guid_two)
		
		statement = statement.filter(relationships_table.relationship == 'member')
		
		statement = statement.statement
		
		
		get_groups_sizes = pd.read_sql(statement, conn).groupby("name").count()
		
		return get_groups_sizes
		
	
	
class entities(object):
	
	
	
	def getall():

		entities_table = Base.classes.elggentities
		entities_query = session.query(entities_table).statement

		get_it_all = pd.read_sql(entities_query, conn)
		
		return get_it_all
	
	def filter_(filter_condition):

		entities_table = Base.classes.elggentities

		entities_session = session.query(entities_table)
		
		entities_ = pd.read_sql(entities_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return entities_
		
		
class metadata(object):
	
	
	def get_all():
		metadata_table = Base.classes.elggmetadata
		metadata_query = session.query(metadata_table).statement
		get_it_all = pd.read_sql(metadata_query, conn)
		
		return get_it_all
	
	def filter_(filter_condition):

		metadata_table = Base.classes.elggmetadata
		metadata_session = session.query(metadata_table)
		
		metadatas = pd.read_sql(metadata_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return metadatas
	

class metastrings(object):
	
	
	
	def get_all():
		
		metastrings_table = Base.classes.elggmetastrings
		
		metastrings_query = session.query(metastrings_table).statement

		get_it_all = pd.read_sql(metastrings_query, conn)
		
		return get_it_all
	
	def filter_(filter_condition):

		metastrings_table = Base.classes.elggmetastrings

		metastrings_session = session.query(metastrings_table)
		
		metastring = pd.read_sql(metastrings_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return metastring


class relationships(object):
	


	def get_all():

		relationships_table = Base.classes.elggentity_relationships

		relationships_query = session.query(relationships_table).statement

		get_it_all = pd.read_sql(relationships_query, conn)
		
		return get_it_all
	
	
	def filter_(filter_condition):
		
		relationships_table = Base.classes.elggentity_relationships
		relationships_session = session.query(relationships_table)
		
		relationship = pd.read_sql(relationships_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return relationship
		


class annotations(object):

	
	def get_all():

		annotations_table = Base.classes.elggannotations

		annotations_query = session.query(annotations_table).statement

		get_it_all = pd.read_sql(annotations_query, conn)
		
		return get_it_all
	
	def filter_(filter_condition):

		annotations_table = Base.classes.elggannotations

		annotations_session = session.query(annotations_table)
		
		annotation = pd.read_sql(annotations_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return annotation
		
		
class objectsentity(object):
	
	

	def get_all():

		objectsentity_table = Base.classes.elggobjects_entity
		
		objectsentity_query = session.query(objectsentity_table).statement

		get_it_all = pd.read_sql(elggobjects_query, conn)


		
		return get_it_all
	
	def filter_(filter_condition):

		objectsentity_table = Base.classes.elggobjects_entity
		
		objectsentity_session = session.query(objectsentity_table)	

		elggobject = pd.read_sql(elggobjects_session.filter(text("{}".format(filter_condition))).statement, conn)
		
		return elggobject  
		
		
class micromissions(object): # Not fully operational yet.
	


	def get_users(): # From here you can use dataframe groupbys and filters if you so choose


		users_table = Base.classes.elggusers_entity
		metadata_table = Base.classes.elggmetadata
		metastrings_table = Base.classes.elggmetastrings
		
		metastrings_table_2 = aliased(metastrings_table)
		metadata_table_2 = aliased(metadata_table)
		
		statement = session.query(users_table.guid, users_table.name, users_table.email,
								  metadata_table.name_id, metadata_table.value_id, metadata_table_2.value_id,
								  metastrings_table,
								  metastrings_table_2)
		
		
		statement = statement.filter(users_table.guid == metadata_table.entity_guid)
		statement = statement.filter(metadata_table_2.entity_guid == users_table.guid)
		statement = statement.filter(metastrings_table.id == metadata_table.value_id)
		statement = statement.filter(metastrings_table_2.id == metadata_table_2.value_id)
		statement = statement.filter(metadata_table_2.name_id == 8667)        
		statement = statement.filter(metadata_table.name_id == 1192767)



		statement = statement.statement
		
		get_it_all = pd.read_sql(statement, conn)
		
		get_it_all.columns = ['guid', 'name', 'email', 'md1_name_id', 'md1_value_id', 'md2_value_id',
							  'ms1_id', 'opt-in', 'ms2_id', 'department']
		
		return get_it_all
	
	def get_aggregate(): 

		metadata_table = Base.classes.elggmetadata
		metastrings_table = Base.classes.elggmetastrings
		
		statement = session.query(metastrings_table.string, metadata_table.entity_guid)
		statement = statement.filter(metastrings_table.id == metadata_table.value_id)
		
		statement = statement.filter(metadata_table.name_id == 1192767)
		
		statement = statement.statement
		
		get_it_all = pd.read_sql(statement, conn).groupby("string").count()
		get_it_all.columns = ['Count']


		return get_it_all
		
		
		
class content(object):
	

	
	# Likes are tough to find, but I think I done did it.
	
	# Blogs, Files, Discussions, would all be in this class.
	
	# The tough part is making sure everything I'm doing is actually correct, especially differentiating the entities
	
	# within the subtypes are the actual topics themselves, and not just the metadata
	
	# And would need (comments = True, tags = True), like the metadata associated with it.
	
	
	
	def get_blogs(): # Got'em
		
		entities_table = Base.classes.elggentities
		objectsentity_table = Base.classes.elggobjects_entity

		statement = session.query(entities_table, objectsentity_table)
		
		statement = statement.filter(entities_table.guid == objectsentity_table.guid)
		statement = statement.filter(entities_table.subtype == 5)
		
		statement = statement.statement
		
		get_blogs = pd.read_sql(statement, conn)
		
		
		return get_blogs
	
	
	def get_discussions():
		

		entities_table = Base.classes.elggentities
		objectsentity_table = Base.classes.elggobjects_entity

		statement = session.query(entities_table, objectsentity_table)
		
		statement = statement.filter(entities_table.guid == objectsentity_table.guid)
		statement = statement.filter(entities_table.subtype == 7)
		
		statement = statement.statement
		
		get_discussions = pd.read_sql(statement, conn)
		
		
		return get_discussions
	
	def get_files():

		entities_table = Base.classes.elggentities
		objectsentity_table = Base.classes.elggobjects_entity
		
		statement = session.query(entities_table, objectsentity_table)
		
		statement = statement.filter(entities_table.guid == objectsentity_table.guid)
		statement = statement.filter(entities_table.subtype == 1)
		
		statement = statement.statement
		
		get_files = pd.read_sql(statement, conn)
		
		
		return get_files
	
	def get_bookmarks():

		entities_table = Base.classes.elggentities
		objectsentity_table = Base.classes.elggobjects_entity
		
		statement = session.query(entities_table, objectsentity_table)
		
		statement = statement.filter(entities_table.guid == objectsentity_table.guid)
		statement = statement.filter(entities_table.subtype == 8)
		
		statement = statement.statement
		
		get_bookmarks = pd.read_sql(statement, conn)
		
		
		return get_bookmarks
	
	
	def get_ideas():

		entities_table = Base.classes.elggentities
		objectsentity_table = Base.classes.elggobjects_entity
		
		statement = session.query(entities_table, objectsentity_table)
		
		statement = statement.filter(entities_table.guid == objectsentity_table.guid)
		statement = statement.filter(entities_table.subtype == 42)
		
		statement = statement.statement
		
		get_ideas = pd.read_sql(statement, conn)
		
		
		return get_ideas

