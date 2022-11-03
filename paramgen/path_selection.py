import duckdb
import argparse
from pathlib import Path
from datetime import timedelta, datetime, timezone
import time
import itertools
import networkit as nk
from networkit.dynamics import GraphEvent
import pandas as pd
import numpy as np

class PathCuration():
    """
    Curates the people4Hops factor table to select valid 4-hop paths
    per day for each day. This class loads the Person and Person_knows_Person
    tables into a networkit graph data structure at which it is updated per
    batch time window. At each time window, each person-pair from the
    people4Hops factor table is checked whether a valid 4-hop path exists
    using bidirectional BFS.
    """
    def __init__(self, data_path, factor_table_path, hop_count=4):
        """
        Args:
            - data_path (str): Path to the raw parquet directory containing
                               the 'dynamic' folder
            - factor_table_path (str): Path to the factor tables.
        """
        Path('scratch/path_selection.duckdb').unlink(missing_ok=True)
        self.cursor = duckdb.connect(database="scratch/path_selection.duckdb")
        self.G = None
        self.HOP_COUNT = hop_count
        self.APSP = None
        self.data_path = data_path
        self.factor_table_path = factor_table_path
        self.node_map = {}
        self.node_map_inverted = {}

    def create_views(self):
        """
        Creates views over the parquet files used for path curation.
        """

        person_parquets = "/graphs/parquet/raw/composite-merged-fk/dynamic/Person/*.parquet"
        knows_parquets = "/graphs/parquet/raw/composite-merged-fk/dynamic/Person_knows_Person/*.parquet"

        people4hops_parquets = "/people4Hops/*.parquet"

        self.cursor.execute(f"DROP VIEW IF EXISTS person")
        self.cursor.execute(f"DROP VIEW IF EXISTS people4Hops")
        self.cursor.execute(f"DROP VIEW IF EXISTS knows")
        person_path = str(Path(self.data_path).absolute()) + person_parquets
        self.cursor.execute(
            f"""
            CREATE VIEW person AS
            SELECT * FROM read_parquet('{person_path}');
            """
        )
        knows_path = str(Path(self.data_path).absolute()) + knows_parquets
        self.cursor.execute(
            f"""
            CREATE VIEW knows AS
            SELECT * FROM read_parquet('{knows_path}');
            """
        )
        people_4_hops_path = str(Path(self.factor_table_path).absolute()) + people4hops_parquets
        self.cursor.execute(
            f"""
            CREATE VIEW people4Hops AS
            SELECT * FROM read_parquet('{people_4_hops_path}');
            """
        )

    def add_nodes_to_graph_init(self):
        """
        Adds all the persons from the Person table. Creates a dictionary
        to map the personIds with the node ids in the graph.
        Returns:
            Number of nodes in the graph at initialization.
        """
        all_nodes = self.cursor.execute(
            f"""
            SELECT id FROM person;
            """
        ).fetchall()
        all_nodes = list(itertools.chain(*all_nodes))
        self.node_map = {k: v for v, k in enumerate(all_nodes)}
        self.node_map_inverted = {v: k for v, k in enumerate(all_nodes)}

        # Initialize the graph nodes
        all_nodes_count = len(all_nodes)
        self.G = nk.Graph(all_nodes_count)

        return all_nodes_count

    def remove_nodes(self, start_date_long, end_date_long):
        """
        Get the remove nodes events from Person table.
        Args:
            - start_date_long (int): start day of the window
            - end_date_long (int): end day of the window
        Returns:
            tuple (amount of delete events, list of GraphEvent objects)
        """
        nodes_deleted = self.cursor.execute(
            f"""
            SELECT id FROM person
            WHERE deletionDate > {start_date_long}
              AND deletionDate < {end_date_long};
            """
        ).fetchall()

        delete_ids = [self.node_map.get(key) for key in list(itertools.chain(*nodes_deleted))]
        delete_events = [
            nk.dynamics.GraphEvent(
                GraphEvent.NODE_REMOVAL,
                id,
                0,
                1.0
            ) for id in delete_ids]

        return len(nodes_deleted), delete_events

    def add_edges(self, start_date_long, end_date_long):
        """
        Get the create edges events from Person_knows_Person table.
        Args:
            - start_date_long (int): start day of the window
            - end_date_long (int): end day of the window
        Returns:
            tuple (amount of create events, list of GraphEvent objects)
        """
        edges_created = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM knows
            WHERE creationDate > {start_date_long}
              AND creationDate < {end_date_long};
            """
        ).fetchall()

        edges_created_events = [
            nk.dynamics.GraphEvent(
                GraphEvent.EDGE_ADDITION,
                self.node_map[node_a],
                self.node_map[node_b],
                1.0
            ) for node_a, node_b in edges_created]

        return len(edges_created), edges_created_events

    def remove_edges(self, start_date_long, end_date_long):
        """
        Get the remove edges events from Person_knows_Person table.
        Args:
            - start_date_long (int): start day of the window
            - end_date_long (int): end day of the window
        Returns:
            tuple (amount of delete events, list of GraphEvent objects)
        """
        edges_deleted = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM knows
            WHERE explicitlyDeleted = true
              AND deletionDate > {start_date_long}
              AND deletionDate < {end_date_long};
            """
        ).fetchall()
        edges_deleted_events = [
            nk.dynamics.GraphEvent(
                GraphEvent.EDGE_REMOVAL,
                self.node_map[node_a],
                self.node_map[node_b],
                1.0
            ) for node_a, node_b in edges_deleted]
        return len(edges_deleted), edges_deleted_events

    def init_graph(self, start_date, end_date):
        """
        Initializes the graph data structure.
        All the person nodes are added so mapping between the personIds and
        node ids from the graph can be stored. We only remove nodes when
        the deletion date is approached and the edges are added upon
        creationDate.
        Args:
            - start_date (datetime.datetime): start day of the window
            - end_date (datetime.datetime): end day of the window
        Returns:
            Dictionary with graph initialization statistics
        """
        start_date_long = start_date.timestamp() * 1000
        end_date_long = end_date.timestamp() * 1000

        print("------------ Adding Nodes (init) ------------")
        total_nodes = self.add_nodes_to_graph_init()

        print("------------ Deleting Nodes (init) ------------")
        nodes_removed, delete_events = self.remove_nodes(start_date_long, end_date_long)

        print("------------ Adding Edges (init) ------------")
        edges_added, edges_added_events = self.add_edges(start_date_long, end_date_long)

        print("------------ Removing Edges (init) ------------")
        edges_removed, edges_removed_events = self.remove_edges(start_date_long, end_date_long)

        print("------------ Updating Graph (init) ------------")
        updater = nk.dynamics.GraphUpdater(self.G)
        updater.update(edges_added_events)
        updater.update(edges_removed_events)
        updater.update(delete_events)
        print("------------ Graph Updated (init) ------------")

        return {
            "start_date"    : start_date,
            "end_date"      : end_date,
            "nodes_added"   : total_nodes,
            "nodes_removed" : nodes_removed,
            "edges_added"   : edges_added,
            "edges_removed" : edges_removed
        }

    def update_graph(self, start_date, end_date):
        """
        Updates the graph with the updates within the given time window.
        Args:
            - start_date (datetime.datetime): start day of the window
            - end_date (datetime.datetime): end day of the window
        Returns:
            Dictionary with update statistics
        """

        start_date_long = start_date.timestamp() * 1000
        end_date_long = end_date.timestamp() * 1000

        print("------------ Deleting Nodes ------------")
        nodes_removed, delete_events = self.remove_nodes(start_date_long, end_date_long)

        print("------------ Adding Edges ------------")
        edges_added, edges_added_events = self.add_edges(start_date_long, end_date_long)

        print("------------ Removing Edges ------------")
        edges_removed, edges_removed_events = self.remove_edges(start_date_long, end_date_long)

        print("------------ Updating Graph ------------")
        updater = nk.dynamics.GraphUpdater(self.G)
        updater.update(edges_added_events)
        updater.update(edges_removed_events)
        updater.update(delete_events)
        print("------------ Graph Updated ------------")

        return {
            "start_date"    : start_date,
            "end_date"      : end_date,
            "nodes_removed" : nodes_removed,
            "edges_added"   : edges_added,
            "edges_removed" : edges_removed
        }

    def get_node_pairs(self):
        """
        Reads the personIds from the people4hops factor table.
        Returns list of tuples with the personIds mapped to the ids of the
        nodes in the graph.
        """
        people_4_hops = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM people4Hops;
            """
        ).fetchall()

        mapped_pairs = []
        for node_a, node_b in people_4_hops:
            mapped_pairs.append((self.node_map[node_a], self.node_map[node_b]))
        return mapped_pairs

    def run(self, start_date, end_date, time_bucket_size_in_days):
        """
        Checks the people4Hops factor table for available paths in given time
        window.
        Args:
            - start_date: Start date of the parameter curation
            - end_date: End date of the parameter curation
            - time_bucket_size_in_days (int): The amount of days in a bucket, e.g. 1
        Returns:
            List of dicts {person1Id, person2Id, useFrom, useUntil}
        """
        paths = []
        self.create_views()
        init_date = datetime(year=1970, month=1, day=1, tzinfo=timezone.utc)

        print("------------ Graph Initialization ------------")
        start_init = time.time()
        stats_dict = self.init_graph(init_date, start_date)
        end_init = time.time()
        print(stats_dict)
        duration_init = end_init - start_init
        print(f"------------ Graph Init Duration {duration_init:.4f} seconds ------------")
        print("------------ Graph Initialized ------------")

        date_limit = date_limit_end = start_date
        window_time = timedelta(days=time_bucket_size_in_days)
        date_limit_end = date_limit_end + window_time

        start_calculation = time.time()
        people_4_hops = self.get_node_pairs()
        while (date_limit_end < end_date):
            print(f"Dates: {date_limit} - {date_limit_end}")
            paths_discarded = paths_found = 0
            _ = self.update_graph(date_limit, date_limit_end)
            for node_a, node_b in people_4_hops:
                # Some nodes in the people4Hops table are already deleted
                # before the benchmark time window requiring checking if
                # the nodes exist.
                if self.G.hasNode(node_a) and self.G.hasNode(node_b):
                    total_hops = nk.distance.BidirectionalBFS(self.G, node_a, node_b).run().getDistance()
                    if (total_hops == self.HOP_COUNT):
                        # The timedelta of 1 day is to prevent that the update
                        # on the day itself is after the issuing of the parameter
                        paths.append(
                            {
                                "person1Id" : self.node_map_inverted[node_a],
                                "person2Id" : self.node_map_inverted[node_b],
                                "useFrom" : date_limit + timedelta(days=1),
                                "useUntil" : date_limit_end + timedelta(days=1)
                            }
                        )
                        paths_found += 1
                    else:
                        paths_discarded += 1
            print(f"Total paths discarded: {paths_discarded}. Total found: {paths_found}")
            date_limit = date_limit + window_time
            date_limit_end = date_limit_end + window_time
        end_calculation = time.time()
        duration_calculation = end_calculation - start_calculation
        print(f"Path calculation duration: {duration_calculation:.4f} seconds")
        print(f"Total duration: {duration_calculation + duration_init:.4f} seconds")

        return paths

    def get_people_4_hops_paths(
        self,
        start_date,
        end_date,
        time_bucket_size_in_days:int,
        parquet_output_dir:str=None,
    ):
        """
        Entry point function of the PathCuration class.
        Get valid 4-hop path per day. Outputs the paths to a parquet file.
        Args:
            - start_date: Start date of the parameter curation
            - end_date: End date of the parameter curation
            - time_bucket_size_in_days (int): The amount of days in a bucket, e.g. 1
            - parquet_output_dir (str, optional): Path to store the parquet file, e.g. scratch/factors/path_curated.parquet
        Returns:
            Dataframe with curated paths
        """
        list_of_paths = self.run(start_date, end_date, time_bucket_size_in_days)

        df = pd.DataFrame(list_of_paths)
        df = df.sort_values(['person1Id','person2Id'])
        day_diff = (df['useFrom'] - df['useUntil'].groupby([df['person1Id'], df['person2Id']]).shift()).dt.days
        group_no = (day_diff.isna() | day_diff.gt(1)).cumsum()
        df_out = (df.groupby(['person1Id','person2Id', group_no], dropna=False, as_index=False)
            .agg({'person1Id': 'first',
                  'person2Id': 'first',
                  'useFrom': 'first',
                  'useUntil': lambda x: x.iloc[-1],
                }))

        df_out['useFrom'] = df_out['useFrom'].astype(np.int64) // 10**6
        df_out['useUntil'] = df_out['useUntil'].astype(np.int64) // 10**6

        df_out['useFrom'] = pd.to_datetime(df_out['useFrom'])
        df_out['useUntil'] = pd.to_datetime(df_out['useUntil'])

        if (parquet_output_dir):
            self.cursor.execute("""
            CREATE TABLE paths_curated (
                person1Id bigint,
                person2Id bigint,
                useFrom timestamp,
                useUntil timestamp
            );
            """)
            self.cursor.execute("INSERT INTO paths_curated SELECT * FROM df_out")
            self.cursor.execute(f"COPY (SELECT * FROM paths_curated ORDER BY UseFrom ASC) TO '{parquet_output_dir}' WITH (FORMAT PARQUET);")

        return df_out

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--raw_parquet_dir',
        help="raw_parquet_dir: directory containing the data e.g. 'graphs/parquet/raw/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--factor_tables_dir',
        help="factor_tables_dir: directory containing the factor tables e.g. '/data/out-sf1'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--output_dir',
        help="output_dir: folder to output the data",
        type=str,
        required=True
    )
    args = parser.parse_args()

    path_curation = PathCuration(args.raw_parquet_dir, args.factor_tables_dir)
    start_date = datetime(year=2012, month=11, day=28, tzinfo=timezone.utc)
    end_date = datetime(year=2013, month=1, day=1, tzinfo=timezone.utc)
    path_curation.get_people_4_hops_paths(start_date, end_date, 1, args.output_dir)
