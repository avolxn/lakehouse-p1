import json
from superset.app import create_app
from superset import db

app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    database = db.session.query(Database).filter_by(database_name="Lakehouse Trino").one_or_none()
    if database is None:
        database = Database(database_name="Lakehouse Trino", sqlalchemy_uri="trino://student@trino:8080/iceberg/bronze", expose_in_sqllab=True)
        db.session.add(database)
        db.session.commit()
    for table in ("customer", "orders", "lineitem"):
        dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, schema="bronze", table_name=table).one_or_none()
        if dataset is None:
            dataset = SqlaTable(database=database, schema="bronze", table_name=table)
            db.session.add(dataset)
            db.session.commit()
            dataset.fetch_metadata()
            db.session.commit()
    sql = "SELECT 'customer' AS table_name,count(*) AS rows FROM iceberg.bronze.customer UNION ALL SELECT 'orders',count(*) FROM iceberg.bronze.orders UNION ALL SELECT 'lineitem',count(*) FROM iceberg.bronze.lineitem"
    frame = database.get_df(sql, catalog="iceberg", schema="bronze")
    print("BRONZE_PROOF=" + frame.to_json(orient="records"))
