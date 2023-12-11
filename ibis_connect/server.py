import argparse
import random
import string
import json
import pickle
import pyarrow
import pyarrow.flight
import ibis

class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.host = host
        self.backend = ibis.get_backend()
        self.flights = {}

    def list_flights(self, context, criteria):
        pass

    def _make_flight_info(self, payload, schema, num_rows, descriptor):
        random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
        location = pyarrow.flight.Location.for_grpc_tcp(self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(random_str, [location]), ]
        self.flights[random_str] = payload
        return pyarrow.flight.FlightInfo(schema, descriptor, endpoints, num_rows, -1)


    def get_flight_info(self, context, descriptor):
        random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
        try:
            command = descriptor.command.decode()
            command = json.loads(command)
            is_plan = False
        except:
            is_plan = True

        if is_plan:
            expr = pickle.loads(descriptor.command)
            schema = pyarrow.Table.from_pandas(self.backend.execute(expr=expr.limit(0))).schema
            return self._make_flight_info(expr, schema, -1, descriptor)

        elif command['command'] == 'list_tables':
            table = pyarrow.Table.from_arrays(
                arrays=[pyarrow.array(self.backend.list_tables(), pyarrow.string())],
                names=['table']
            )
            return self._make_flight_info(table, table.schema, table.num_rows, descriptor)
        elif command['command'] == 'get_table':
            schema = self.backend.table(command['name'], command['database']).to_pyarrow(limit=0).schema
            return pyarrow.flight.FlightInfo(schema, descriptor, [], 0, -1)

    def do_put(self, context, descriptor, reader, writer):
        command = descriptor.command.decode()
        command = json.loads(command)
        if command['command'] == 'create_table':
            table = reader.read_all()
            self.backend.create_table(
                command['name'], 
                table,
                #TODO schema=
                database=command['database'],
                temp=command['temp'],
                overwrite=command['overwrite']
            )

    def do_get(self, context, ticket):
        key = ticket.ticket.decode()
        if key in self.flights:
            flight = self.flights[key]

            if type(flight) == ibis.expr.types.relations.Table:
                table = self.backend.to_pyarrow(flight)
            else:
                table = flight

            return pyarrow.flight.RecordBatchStream(table)

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost",
                        help="Address or hostname to listen on")
    parser.add_argument("--port", type=int, default=5005,
                        help="Port number to listen on")

    args = parser.parse_args()
    scheme = "grpc+tcp"

    location = "grpc+tcp://{}:{}".format(args.host, args.port)

    server = FlightServer(args.host, location)
    print("Serving on", location)
    server.serve()


if __name__ == '__main__':
    main()