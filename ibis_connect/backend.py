import ibis
import pandas as pd
import pyarrow as pa
import pickle
import json
from typing import Any
import ibis.expr.types as ir
from ibis.expr import operations as ops
from ibis.backends.base import BaseBackend
import pyarrow.flight
from pyarrow.flight import FlightClient

class ConnectTable(ir.Table):
    pass


class ConnectBackend(BaseBackend):
    def __init__(self, location, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flight_client: FlightClient = pyarrow.flight.FlightClient(location)

    name = "connect"

    def create_table(
        self,
        name: str,
        obj: pd.DataFrame | pa.Table | ir.Table | None = None,
        *,
        schema: ibis.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        if obj is not None:
            command = {
                'command': 'create_table',
                'name': name,
                'database': database,
                'temp': temp,
                'overwrite': overwrite
            }

            if type(obj) == pd.DataFrame:
                py_table = pyarrow.Table.from_pandas(obj)

                command = json.dumps(command).encode()
                writer, _ = self.flight_client.do_put(
                    pyarrow.flight.FlightDescriptor.for_command(command), py_table.schema)
                writer.write_table(py_table)
                writer.close()

    def create_view(
        self,
        name: str,
        obj: ir.Table,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> ir.Table:
        pass

    def drop_table(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> None:
        pass

    def drop_view(
        self, name: str, *, database: str | None = None, force: bool = False
    ) -> None:
        pass

    def list_tables(
        self, like: str | None = None, database: str | None = None
    ) -> list[str]:
        command = {
            'command': 'list_tables',
            'like': like,
            'database': database
        }

        command = json.dumps(command).encode()
    
        flight_info = self.flight_client.get_flight_info(
            pyarrow.flight.FlightDescriptor.for_command(command))
        
        result = []
        for endpoint in flight_info.endpoints:
            for location in endpoint.locations:
                get_client = pyarrow.flight.FlightClient(location)
                reader = get_client.do_get(endpoint.ticket)
                tables = list(reader.read_pandas()['table'])
                result.extend(tables)

        return result

    def table(self, name: str, database: str | None = None) -> ir.Table:
        command = {
            'command': 'get_table',
            'name': name,
            'database': database
        }

        command = json.dumps(command).encode()

        flight_info = self.flight_client.get_flight_info(
            pyarrow.flight.FlightDescriptor.for_command(command))
        
        node = ops.DatabaseTable(
            name, flight_info.schema, self #, namespace=ops.Namespace(database=database)
        )
        return ConnectTable(node)

    def version(self) -> str:
        pass

    def to_pyarrow(self, expr: ir.Expr, **kwargs: Any) -> Any:        
        command = pickle.dumps(expr.unbind())

        flight_info = self.flight_client.get_flight_info(
            pyarrow.flight.FlightDescriptor.for_command(command))

        tables = []

        for endpoint in flight_info.endpoints:
            for location in endpoint.locations:
                get_client = pyarrow.flight.FlightClient(location)
                reader = get_client.do_get(endpoint.ticket)
                tables.append(reader.read_all())

        return pyarrow.concat_tables(tables)

    def execute(self, expr: ir.Expr, **kwargs: Any) -> Any:
        return self.to_pyarrow(expr=expr, **kwargs).to_pandas()
