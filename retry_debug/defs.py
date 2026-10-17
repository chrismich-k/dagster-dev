import dagster as dg


@dg.op
def boom():
    raise Exception("boom")


@dg.job
def fail_job():
    boom()


defs = dg.Definitions(jobs=[fail_job])
