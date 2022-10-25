import duckdb
from pathlib import Path
from datetime import timedelta, datetime
import time
import itertools
import networkit as nk
from networkit.dynamics import GraphEvent
import pandas as pd

class PathCuration():

    def __init__(self, data_path, factor_table_path):
        Path('scratch/path_selection.duckdb').unlink(missing_ok=True)
        self.cursor = duckdb.connect(database="scratch/path_selection.duckdb")
        # We first initialize using networkx so we can add nodes with given id
        # then we convert it to networkit once graph is initialized (see update_graph)
        self.G = None
        self.APSP = None
        self.data_path = data_path
        self.factor_table_path = factor_table_path
        self.node_map = {}
        self.node_map_inverted = {}

    def create_views(self):
        person_parquets = "/dynamic/Person/*.parquet"
        knows_parquets = "/dynamic/Person_knows_Person/*.parquet"

        people4hops_parquets = "/people4Hops/*.parquet"

        self.cursor.execute(f"DROP VIEW IF EXISTS person")
        self.cursor.execute(f"DROP VIEW IF EXISTS people4Hops")
        self.cursor.execute(f"DROP VIEW IF EXISTS knows")
        self.cursor.execute(
            f"""
            CREATE VIEW person AS 
            SELECT * FROM read_parquet('{str(Path(self.data_path).absolute()) + person_parquets}');
            """
        )
        self.cursor.execute(
            f"""
            CREATE VIEW knows AS 
            SELECT * FROM read_parquet('{str(Path(self.data_path).absolute()) + knows_parquets}');
            """
        )
        self.cursor.execute(
            f"""
            CREATE VIEW people4Hops AS 
            SELECT * FROM read_parquet('{str(Path(self.factor_table_path).absolute()) + people4hops_parquets}');
            """
        )

    def add_nodes_to_graph_init(self):
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
        nodes_deleted = self.cursor.execute(
            f"""
            SELECT id FROM person
            WHERE deletionDate > {start_date_long}
              AND deletionDate < {end_date_long};
            """
        ).fetchall()

        delete_ids = [self.node_map.get(key) for key in list(itertools.chain(*nodes_deleted))]
        delete_events = [nk.dynamics.GraphEvent(GraphEvent.NODE_REMOVAL, id, 0, 1.0) for id in delete_ids]

        return len(nodes_deleted), delete_events

    def add_edges(self, start_date_long, end_date_long):
        edges_created = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM knows
            WHERE creationDate > {start_date_long}
              AND creationDate < {end_date_long};
            """
        ).fetchall()

        edges_created_events = [nk.dynamics.GraphEvent(GraphEvent.EDGE_ADDITION, self.node_map[edge[0]], self.node_map[edge[1]], 1.0) for edge in edges_created]

        return len(edges_created), edges_created_events

    def remove_edges(self, start_date_long, end_date_long):
        edges_deleted = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM knows
            WHERE explicitlyDeleted = true
              AND deletionDate > {start_date_long}
              AND deletionDate < {end_date_long};
            """
        ).fetchall()
        edges_deleted_events = [nk.dynamics.GraphEvent(GraphEvent.EDGE_REMOVAL, self.node_map[edge[0]], self.node_map[edge[1]], 1.0) for edge in edges_deleted]
        return len(edges_deleted), edges_deleted_events

    def init_graph(self, start_date, end_date):
        """
        All the nodes are added to get the mapping right. We only remove nodes when
        the deletion date is approached and the links are added when that event is
        scheduled.
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
        Select entities between two dates.
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
        people_4_hops = self.cursor.execute(
            f"""
            SELECT Person1Id, Person2Id FROM people4Hops;
            """
        ).fetchall()

        mapped_pairs = []
        for pair in people_4_hops:
            mapped_pairs.append((self.node_map[pair[0]], self.node_map[pair[1]]))
        return mapped_pairs

    def run(self, start_date:str, end_date:str, time_bucket_size_in_days):
        """
        Runs the path curation script in the following steps:
        1. Create views on the parquet files using DuckDB
        2. Initialize the graph with NetworkKit
        3. 
        """
        paths = []

        self.create_views()
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        init_date = datetime(year=1970, month=1, day=1)

        # Initialize graph
        stats_dict = self.init_graph(init_date, start_date)
        print(stats_dict)
        print("------------ Graph Initialized ------------")

        date_limit = date_limit_end = start_date
        window_time = timedelta(days=time_bucket_size_in_days)
        date_limit_end = date_limit_end + window_time

        people_4_hops = self.get_node_pairs()
        start_total = time.time()
        while (date_limit_end < end_date):
            paths_discarded = 0
            print(f"Dates: {date_limit} - {date_limit_end}")
            _ = self.update_graph(date_limit, date_limit_end)
            for node_pair in people_4_hops:
                if self.G.hasNode(node_pair[0]) and self.G.hasNode(node_pair[1]):
                    total_hops = nk.distance.BidirectionalBFS(self.G, node_pair[0], node_pair[1]).run().getHops()
                    if (total_hops == 4):
                        # The timedelta of 1 day is to prevent that the update on the day
                        # itself is after the issuing of the parameter
                        paths.append(
                            {
                                "person1Id" : self.node_map_inverted[node_pair[0]],
                                "person2id" : self.node_map_inverted[node_pair[1]],
                                "useFrom" : date_limit + timedelta(days=1),
                                "useUntil" : date_limit_end + timedelta(days=1)
                            }
                        )
                    else:
                        paths_discarded += 1
            print(f"Total paths discarded: {paths_discarded}")
            date_limit = date_limit + window_time
            date_limit_end = date_limit_end + window_time
        end_total = time.time()
        duration_calculation = end_total - start_total
        print(f"Generation of paths duration: {duration_calculation:.4f} seconds")

        return paths

    def get_people_4_hops_paths(
        self, start_date:str,
        end_date:str,
        time_bucket_size_in_days:int,
        parquet_output_dir:str
    ):
        list_of_paths = self.run(start_date, end_date, time_bucket_size_in_days)

        df = pd.DataFrame(list_of_paths)
        df = df.groupby(['person1Id', 'person2id']).agg({'useFrom': ['min'], 'useUntil': ['max']}).reset_index()
        df.columns = df.columns.get_level_values(0)
        self.cursor.execute("CREATE TABLE paths_curated AS SELECT * FROM df")
        self.cursor.execute(f"COPY paths_curated TO '{parquet_output_dir}' WITH (FORMAT PARQUET);")

# if __name__ == "__main__":
#     # parser = argparse.ArgumentParser()
#     # parser.add_argument(
#     #     '--raw_parquet_dir',
#     #     help="raw_parquet_dir: directory containing the raw parquet files e.g. '/data/out-sf1'",
#     #     type=str,
#     #     default='factors/',
#     #     required=False
#     # )
#     # parser.add_argument(
#     #     '--start_date',
#     #     help="start_date: Start date of the update streams, e.g. '2012-11-28'",
#     #     type=str,
#     #     default='2012-11-28',
#     #     required=False
#     # )

#     # args = parser.parse_args()

#     # path = "/home/gladap/repos/ldbc-data/spark/ldbc_snb_interactive_sf10/initial_snapshot/graphs/parquet/raw/composite-merged-fk/"
#     # factor_path = "/home/gladap/repos/ldbc-data/spark/ldbc_snb_interactive_sf10/initial_snapshot/factors/parquet/raw/composite-merged-fk/"
#     path_curation = PathCuration(path, factor_path)
#     df = path_curation.get_people_4_hops_paths('2012-11-28', '2013-01-01', 1)

#     # main(args.raw_parquet_dir, args.start_date)