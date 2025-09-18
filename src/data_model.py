from typing import overload, Literal
import json
import requests
from sqlalchemy import Integer, Numeric, Float, String, Date, DateTime, Time, Text, Boolean, LargeBinary,BigInteger, Table, Column, MetaData, create_engine
from sqlalchemy.schema import CreateTable

class DataModel():
    @overload
    def __init__(
        self, 
        mode: Literal["data-models-service"], 
        name: str, 
        version: str, 
        *, 
        service_base_url: str = "https://data-models-service.research.chop.edu/schemata",
    ) -> None: ...
    
    @overload
    def __init__(
        self, 
        mode: Literal["json"], 
        name: str, 
        version: str, 
        *, 
        file_path: str,
    ) -> None: ...

    def __init__(self, mode, name, version, **kargs):
        """
        Initialize DataModel instance.
        Args:
            mode (str): mode to retrieve data models. Currently support 'data-models-service', 'json'
            name (str): data models name. e.g. pedsnet/pcornet
            version (str): version of data models. e.g. 5.7.0
            **kargs: additional arguments based on mode
                - if mode is 'data-models-service', support 'service_base_url' (str): Base url where data models service is running. Defaults to "https://data-models-service.research.chop.edu/schemata"
                - if mode is 'json', support 'file_path' (str): path to the local json file
        """
        self.mode = mode
        self.name = name
        self.version = version
        if mode == 'data-models-service':
            service_base_url = kargs.get('service_base_url', "https://data-models-service.research.chop.edu/schemata")
            url = '/'.join([service_base_url, name, version + "?format=json"])
            self.source = url
            response = requests.get(url)
            response.raise_for_status()
            self.data = response.json()
        elif mode == 'json':
            file_path = kargs.get('file_path')
            self.source = file_path
            if not file_path:
                raise ValueError("file_path is required when mode is 'json'")
            with open(file_path, 'r') as f:
                self.data = json.load(f)
        else:
            raise ValueError(f"Invalid value for mode: {mode}. Accepted values are: 'data-models-service', 'json'. ")
    
    def all_table_names(self):
        """
        Get a list of all table names in the data model.
        """
        return [table_item['name'] for table_item in self.data['tables']]
    

    def all_column_names_in_table(self, table_name: str) -> list:
        """
        Get a list of all column names in a specific table.

        Args:
            table_name (str): The name of the table.
        Returns:
            list: A list of column names in the specified table.
        Raises:
            ValueError: If the specified table is not found in the data model.
        """
        for table_item in self.data['tables']:
            if table_item['name'] == table_name:
                return [field_item['name'] for field_item in table_item['fields']]
        raise ValueError(f"Table '{table_name}' not found in the data model.")
    

    def to_duckdb_ddl(self) -> dict:
        """
        Convert data-models dict object into a dict of duckdb ddls. 

        Returns:
            dict: The output dict key is table_name, value is duckdb dialect ddl for the table. 
        """
        # mapping from data-models datatype to sqlalchemy datatype class
        datatype_map =  {
            'integer': Integer,
            'number': Numeric,
            'decimal': Numeric,
            'float': Float,
            'string': String,
            'date': Date,
            'datetime': DateTime,
            'timestamp': DateTime,
            'time': Time,
            'text': Text,
            'clob': Text,
            'boolean': Boolean,
            'blob': LargeBinary,
            'biginteger': BigInteger
        }
        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()
        output = dict()
        for table_info in self.data['tables']:
            table_name = table_info['name']
            cols = []
            for field_info in table_info['fields']:
                col_kwargs = dict()
                column_name = field_info['name']
                type_str = field_info['type']
                if field_info['length']:
                    col_kwargs['length'] = field_info['length']
                if field_info['precision']:
                    col_kwargs['precision'] = field_info['precision']
                if field_info['scale']:
                    col_kwargs['scale'] = field_info['scale']

                type_class = datatype_map[type_str]
                type_kwargs = dict()
                if type_class == String:
                    # Specifying the length for the VARCHAR, STRING, and TEXT types has no effect on DuckDB. 
                    # Length limit is enforced as a DQ check instead. 
                    # https://duckdb.org/docs/stable/sql/data_types/text.html#specifying-a-length-limit
                    type_kwargs['length'] = col_kwargs.get('length') or 256
                if type_class == Numeric:
                    type_kwargs['precision'] = col_kwargs.get('precision') or 20
                    type_kwargs['scale'] = col_kwargs.get('scale') or 5
                cols.append(Column(column_name, type_class(**type_kwargs)))
            t = Table(table_name, metadata, *cols)
            ddl = str(CreateTable(t).compile(engine))
            output[table_name] = ddl
        return output



    
    
