# cassandra-to-keyspaces
Python script for migrating Cassandra DB to AWS Keyspaces

# How to use it

1) Install requirements `pip install requirements.txt`
2) Put your credentials into `config.ini`
3) Run `./main.py --keyspace={KEYSPACE_TO_MIGRATE}`

All logs by default will go to `replication.log` - watch it.

# List of things we can't migrate

- frozensets   
- indexes
- triggers


# Credits

Sponsored by [Welltory](https://welltory.com)
