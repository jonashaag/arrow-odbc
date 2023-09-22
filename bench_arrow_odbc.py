import os
import arrow_odbc
import tqdm
it = arrow_odbc.read_arrow_batches_from_odbc(
    "select " + 
    # ", ".join(f"cast({c} as varchar)" for c in [
    #     "X", "Y", "Z", "X_noise", "Y_noise", "Z_noise", "R", "G", "B", "time", "eol", "label",
    # ])
    "*"
    + " from bench.dbo.area1",
    2**16,
    "Driver={ODBC Driver 18 for SQL Server};Encrypt=no;Server=localhost;",
    user="sa", password="xxx",
)
input(f"go? {os.getpid()}")
with tqdm.tqdm() as t:
    for b in it:
        t.update(int(len(b)/1e3))
