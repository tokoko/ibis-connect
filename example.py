import ibis
import pandas as pd
from ibis_connect.backend import ConnectBackend

backend = ConnectBackend(location="grpc+tcp://localhost:5005")
ibis.set_backend(backend=backend)

df = pd.DataFrame([[10, 2], [3, 4]], columns=['Col1', 'Col2'])
backend.create_table("mytable", df, overwrite=True)
print(backend.list_tables())
table = backend.table('mytable')
table = table.select('Col1', 'Col2', (table.Col1 - table.Col2).name("Col3"))
print(table.execute())
