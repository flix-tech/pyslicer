# PySlicer v0.2

### ⇒ wtf is that?

Script to cut a piece of a big database in an accurate way.

### ⇒ how it works

It reads the data (record by record) from a source database and puts it into
a target database. For that, it needs two connections for reading and for writing.
Configuration is defined in `config.yml` (check `config.dist.yml` for reference).
User for write connection should have DROP/CREATE/TRUNCATE privileges besides
regular SELECT/INSERT. Data is copied according to set of rules defined in
`schema.yml` (check `schema.dist.yml` for reference).

To copy data it runs several workers at once. Currently up to 8, one worker per table.
Each worker opens own read and write connections. Redis connection is shared.

When copying data it tries to maintain reference integrity by detecting primary
and foreign keys and storing them in interim storage (which is Redis).
After the whole volume of data copied it iterates of sets of foreign keys and copies missing records.

### ⇒ command parameters reference

Required parameters are `read` and `write`, which are names of connections specified in `config.yml`.

Optional:

* `continue` - skip cleaning interim and target storage
* `tables` - narrow copy procedure to a scope of tables (not that references
to out-of-scope tables still will be copied)
* `copy-schema` - runs mysqldump on source database and recreates target one
from its output (table structure and routines, no triggers)

### ⇒ how to use it

You'll need Python 3.2+ and pip for dependencies:

```bash
$ pip3 install -r requirements.txt
$ python3 -u run.py --read=... --write=...
$ mysqldump ... --routines --no-create-db --quick --skip-triggers | sed -E "s/DEFINER=[^ ]+ //g" > /tmp/sliced_db.sql
```

### ⇒ roadmap

It's only v0.2, what's next? It needs a lot of stuff:

* ~~parameters for Redis connection in configuration file~~
* ~~option to set max number workers~~
* ~~option to specify path to schema file~~
* ~~support for several read connections (e.g. different slaves)~~
* pretty names generator based on hash of source value
* export and pack the resulting database
* multi-threading implementation for reference traverse
* fancy progress bar
