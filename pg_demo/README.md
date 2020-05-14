# Description

This is a demo of the extensions `pg_trgm` and `pg_stat_statements` extensions for PostgreSQL.

# Requirements

* A PostgreSQL database started with the `pg_stat_statements` set in the `shared_preload_libraries`
* Python >=3.6 virtual environment

This was not made into a docker container as Python >=3.6 is generally available and the intent was to be able to run this against a database directly on the host, in a container or on RDS or some other service platform.

# Setup

1. Make sure `pip` is up to date: `(venv) $ pip install --upgrade pip`
2. Install required Pyhton packages: `(venv) $ pip install -r ./requirements.txt`

# Usage

The `pg_ext_demo.py` script has only one required argument `db_url` which is a connect string as a URL: `postgresql://<user>:<password>@<host>:<port>/<dbname>`

The credentials for the database **must** be for a **superuser** role.

The `<password>` part can be omitted if you set the environment variable `PGPASSWORD` with the correct password.

# Notes

This is a guided tour through these extensions. The user interaction will be in the form of pressing enter to proceed or scrolling backward in the buffer if information scrolls off-screen.

# External Links

Information on `pg_trgm` can be found [here](https://www.postgresql.org/docs/10/pgtrgm.html).

Information on `pg_stat_statements` can be found [here](https://www.postgresql.org/docs/10/pgstatstatements.html).

A good blog post about text search options by [2ndQuadrant](https://www.2ndquadrant.com/) can be found [here](https://www.2ndquadrant.com/en/blog/text-search-strategies-in-postgresql/).

