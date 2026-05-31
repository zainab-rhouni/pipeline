from dagster import job, op, Nothing, Out, In
import os

@op(out=Out(Nothing))
def ingest():
    os.system("python pipeline/ingest.py")

@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def validate():
    os.system("python pipeline/validate.py")

@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def transform():
    os.system("cd dbt_pipeline && dbt run --profiles-dir .")

@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def test_data():
    os.system("cd dbt_pipeline && dbt test --profiles-dir .")

@job
def ventes_pipeline():
    t1 = ingest()
    t2 = validate(t1)
    t3 = transform(t2)
    test_data(t3)