# GCconnex-Module
Module for easier querying of GConnex Database for Data Analysis


In the GCTools analytics team, a significant challenge is integrating all the tools used in the analysis pipeline in a secure, and efficient fashion. Most significantly, in the GCconnex side of the analytics team, pulling the most vital information from the (Elgg) database proved a tedious challenge. The nature of the database structure meant having to link several tables together to answer a single question, and if we wanted to expand from the question, we would have to go back into the database.

All of this would affect our workflow, as we would have to leave our analysis environment to construct the proper mySQL query. While tools like SQLAlchemy and Pandas would greatly improve our workflow by integrating all aspects of the data analysis project into a Jupyter Notebook, we still found that we could leverage these incredible tools to create a better workflow tailored to our team.

### Goals
The goal of this module is to leverage the existing open source tools to create a tailored syntax for the GCTools analytics team to make integrating more data into the analysis as easy as possible. The core idea of the module is to leverage the flexibility of SQLAlchemy and Pandas by keeping the core functionalities of these modules open to the user at any time, but creating a syntax to automate the tedious, regular queries the team has experienced.

