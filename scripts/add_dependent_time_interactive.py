"""
Script to add dependentTime columns to the update events
for interactive. The method for adding dependent time is
the same as found in the Hadoop Datagen:
https://github.com/ldbc/ldbc_snb_datagen_hadoop/blob/main/src/main/java/ldbc/snb/datagen/serializer/UpdateEventSerializer.java

Required input are the initial_snapshot parquet files together
with the inserts and deletes parquet files.
Outputs the updatestreams in the same parent folder in new folders:
inserts_dep and deletes_dep.
"""

import duckdb
import glob
import os
from constants_dependent import schema_columns, dependent_entity_map
from pathlib import Path

class DependentTimeAppender:

    def __init__(self,
        data_path:str,
        dependent_date_column:str = "dependentDate",
        default_dependent_time:int = 0
    ):
        if (data_path[-1] == '/'):
            data_path = data_path[:-1]

        self.initial_snapshot_path = data_path + "/dynamic"
        self.update_event_path = data_path
        self.dependent_date_column = dependent_date_column
        self.cursor = duckdb.connect(database='snb.duckdb')
        self.default_dependent_time = default_dependent_time
    
    def create_views(self):  
        print("Create temp table")
        with open('./schema_dependentTime.sql') as f:
            schema_def = f.read()
            self.cursor.execute(schema_def)

        print("Creating views")
        for entity in ["Person", "Post", "Comment", "Forum"]:
            self.cursor.execute(f"CREATE VIEW {entity} AS SELECT * FROM read_parquet('{self.initial_snapshot_path}/{entity}/*.parquet');")

    def create_and_load_temp_tables(self):
        """
        Loads the update event data into temporary tables
        """

        for update_type in ['inserts', 'deletes']:
            paths = glob.glob(f'{self.update_event_path}/{update_type}/*.parquet')
            for path in paths:
                operation_type = os.path.basename(path.removesuffix('.parquet'))
                if update_type == 'deletes':
                    operation_type_suffix = "_Delete"
                    date_column = 'deletionDate'
                else:
                    operation_type_suffix = "_Insert"
                    date_column = 'creationDate'
                table_name = operation_type + operation_type_suffix
                print("Parsing: " + table_name)

                # 1. Create select list
                column_string = ""
                for column in schema_columns[table_name]:
                    column_string = column_string + column + ","
                column_string = column_string[:-1] # remove last comma
                
                # 2. Load data into temporary table
                self.cursor.execute(f"INSERT INTO {table_name} SELECT {column_string} FROM read_parquet('" + path + f"')  ORDER BY {date_column} ASC;")
                if (table_name == "Person_Insert"):
                    Path(f"{self.update_event_path}/{update_type}_dep").mkdir(parents=True, exist_ok=True)
                    self.cursor.execute(f"COPY {table_name} TO '{self.update_event_path}/{update_type}_dep/{operation_type}.parquet' (FORMAT PARQUET);")
                    continue
                # 3. Add dependent time for table requiring personIds
                #dependent_entity_map
                self.update_dependent_time(table_name, f'{self.update_event_path}/{update_type}', operation_type)


    def update_dependent_time(self, table_name, output_path, operation_type):
        """
        For each table:
        - Fetch creationDate of the dependent columns
        - Get max of those
        - throw valueError when a value is not found.
        - Store to parquet, drop table and continue
        """
        dependent_dict = dependent_entity_map[table_name]
        dependent_tables = dependent_dict["entity"]
        dependent_column_ids = dependent_dict["eventColumns"]
        entity_columns = dependent_dict["entityColumns"]
        date_column = dependent_dict["dateColumn"]
        i=1
        # We iterate
        select_date_columns = ""
        left_join_tables = ""
        for table, event_id, entity_column in zip(
            dependent_tables,
            dependent_column_ids,
            entity_columns
        ):
            select_date_columns += f"t{i}.{date_column}, "
            left_join_tables += f"LEFT JOIN {table} AS t{i} ON t.{event_id} = t{i}.{entity_column} "
            i+=1
        where_clause = ""
        select_clause = ""

        for match_column in dependent_dict["matchColumns"]:
            select_clause += f"t.{match_column} as {match_column}_match, "
            where_clause += f" {match_column} = {match_column}_match AND"

        where_clause = where_clause[:-4]

        select_date_columns = select_date_columns[:-2] # remove space and comma
        query = f"UPDATE {table_name} SET {self.dependent_date_column} = dependencyTime FROM ("
        query += f"SELECT {select_clause} GREATEST({select_date_columns}) AS dependencyTime "
        query += f"FROM {table_name} AS t "
        query += f" {left_join_tables})"
        query += f" WHERE{where_clause}"
        print(query)
        total_updated = self.cursor.execute(query).fetchall()
        print(f"{table_name} has updated {total_updated}")
        Path(f"{output_path}_dep").mkdir(parents=True, exist_ok=True)
        self.cursor.execute(f"COPY {table_name} TO '{output_path}_dep/{operation_type}.parquet' (FORMAT PARQUET);")
        self.cursor.execute(f"DROP TABLE {table_name};")

    def execute(self, query):
        return self.cursor.execute(query).fetchall()

if __name__ == "__main__":
    root_data_path = '/home/gladap/repos/ldbc-data/spark/out-sf0.1/'
    dta_data_path = root_data_path + 'graphs/parquet/raw/composite-merged-fk'




    DTA = DependentTimeAppender(dta_data_path)
    DTA.create_views()
    DTA.create_and_load_temp_tables()
