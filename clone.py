import subprocess

# Dump from remote
subprocess.run([
    "pg_dump",
    "postgresql://poi:FUJITAAKANE@137.66.10.78:5432/postgres",
    "--schema=hoi",
    "--no-owner",
    "--no-privileges",
    "--format=plain",
    "--file=hoi_schema_dump.sql"
], check=True)

# Restore to local
subprocess.run([
    "psql",
    "postgresql://nymph:ikaros@localhost:5432/amq",
    "-f", "hoi_schema_dump.sql"
], check=True)