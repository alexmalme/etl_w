from prefect import Flow, task
from prefect.utilities.edges import unmapped
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from typing import Dict, Any, Iterable, List, Optional, Union
from datetime import timedelta
from prefect.executors import DaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.environments import DaskKubernetesEnvironment

# mongodb mock up
# Mock up to simulate a MongoDB list with data to connect with SQL Server
stores_parameters: List[Dict] = [
    {
        'brand': 'ViaMia',
        'uid': 223021,
        'queries': [
            {
                'query': 'SELECT * FROM production.products',
                'table_name': 'VIAMIA.PRODUTOS'
            },
            {
                'query': 'SELECT * FROM production.categories',
                'table_name': 'VIAMIA.CATEGORIES'
            }
        ],
        'username': 'SA',
        'password': '8c#laoe#Q&8H@',
        'database': 'master',
        'server': 'localhost',
        'driver': 'ODBC Driver 17 for SQL Server',
        'port': 1700
    },
    {
        'brand': 'Arezzo',
        'uid': 59034772,
        'queries': [
            {
                'query': "SELECT * FROM sales.customers",
                'table_name': 'AREZZO.CUSTOMERS'
            },
            {
                'query': 'SELECT * FROM sales.stores',
                'table_name': 'AREZZO.STORES'
            }
        ],
        'username': 'SA',
        'password': "8c#laoe#Q&8H@",
        'database': "master",
        'server': 'localhost',
        'driver': 'ODBC Driver 17 for SQL Server',
        'port': 1800
    },
    {
        'brand': 'Wrong',
        'uid': 00000000,
        'query': "",
        'username': '',
        'password': "",
        'database': "",
        'server': '',
        'driver': '',
        'port': 1433
    }
]

# Mock up to simulate the SQL Server storage point
ClothesStore_parameters: Dict[str, Any] = {
    'username': 'SA',
    'password': '8c#laoe#Q&8H@',
    'database': 'master',
    'server': 'localhost',
    'driver': 'ODBC Driver 17 for SQL Server',
    'port': 1900
}


@task
def get_params(params: List[Iterable]) -> List[Any]:
    """Auxiliary function to read params as a Prefect task. 

    Args:
        params (List[Iterable]): A List of data to connect with SQL Server

    Returns:
        List[Any]: A List with data to connect with SQL Server.
    """
    return params


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def test_connection(params):
    connection: str = connection_constructor(params)
    conn: Engine = sa.create_engine(connection)
    if conn.connect():
        return params


def connection_constructor(params: Dict[str, Any]) -> str:
    """Receives a dictionary. It will create a connection using sqlalchemy module. 
    
    Args: 
        params (Dict[str, Any]): Dictionary. Values used to create the sqlalchemy engine
    
    Returns:
        connstr (str): String that will be used to create the sqlalchemy engine
    """
    username = params.get('username')
    password = params.get('password')
    server = params.get('server')
    port = params.get('port')
    database = params.get('database')
    driver = params.get('driver')
    connstr: str = (
        f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver={driver}?trusted_connection=yes")
    return connstr


@task
def get_data(params: Dict[str, Any]) -> Dict[str, List[pd.DataFrame]]:
    """Receives a dictionary. It will create a connection using sqlalchemy module.

    Args:
        params (Dict[str, Any]): Dictionary. Used to return the query value

    Returns:
        df (pd.DataFrame): Returns a pandas.DataFrame object.

    """
    connection_string = connection_constructor(params)
    queries: Union[Any, Iterable] = params.get('queries')
    engine: Engine = sa.create_engine(connection_string)
    dfs: Dict = {}
    for item in queries:
        query = item.get('query')
        table_name = item.get('table_name')
        df: pd.DataFrame = pd.read_sql(sql=query, con=engine)
        dfs.setdefault(table_name, [df]).append(df)
    return dfs


@task
def insert_data(params: Dict[str, List], conn_constructor: Dict[str, Any]) -> pd.DataFrame:
    """Creates a sqlalchemy connection engine and inserts data into a 
    table using pandas.DataFrame

    Args:
        dataframe (pd.DataFrame): Pandas.DataFrame object
        params (Dict[str, Any]). Dictionary used to create the sqlalchemy engine        
        table_name (str): SQL Table name
        
    Returns: df(pd.DataFrame): Pandas.Dataframe object
    """
    engine: Engine = sa.create_engine(conn_constructor)
    for table_name in params.keys():
        if table_name:
            for dataframe in params.get(table_name):
                dataframe.to_sql(name=table_name, con=engine,
                                if_exists='append')


with Flow('ClothesStore_Flow6') as f:
    # ClothesStore
    ClothesStore_params: Dict[str, Any] = get_params(ClothesStore_parameters)
    connections_ClothesStore = connection_constructor(params=ClothesStore_parameters)
    # Stores
    stores_params: List[Any] = get_params(stores_parameters)
    # Test connection ClothesStore
    ClothesStore_connection_health = test_connection(params=ClothesStore_params)
    # Test connection Stores
    stores_connection_health = test_connection.map(
        params=stores_params).set_upstream(ClothesStore_connection_health)
    # Get data:
    stores_data = get_data.map(params=stores_connection_health).set_upstream(
        stores_connection_health, mapped=True)
    # Save data:
    insert = insert_data.map(params=stores_data, conn_constructor=unmapped(
        connections_ClothesStore)).set_upstream(stores_data, mapped=True)


#f.visualize()
f.executor = DaskExecutor()
f.run_config = KubernetesRun()
flow.environment = DaskKubernetesEnvironment()
f.register('ClothesStore_Flow6')
#f.run()
