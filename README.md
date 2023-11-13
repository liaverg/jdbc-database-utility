# jdbc-tutorial-database-utility-class
The project provides a custom utility class that simplifies database operations and provides central control of database connection lifecycle and usage.

### Before You Start
Run the SQL Script `scripts\DDL\users.sql` 
in order to create the schema and table that your PostgreSQL database will use. 

Make sure that you configure the `DB_URL`, `USER` and `PASSWORD` fields 
in `src\main\java\com.liaverg\Main.java` so that they match your settings.