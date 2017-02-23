# connection data for the database
db = {
    "database": "",
    "user": "",
    "host": "",
    "port": 5432,
    "password": ""
}

# generating sqlalchemy db url
DB_URL = "postgresql://%s:%s@%s:%s/%s" % (
         db.get('user'), db.get('password'),
         db.get('host'), db.get('port'), db.get('database'))
