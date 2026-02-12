from extract.extract_mysql import extract_all
from transform.transform_book_analytics import transform_book_analytics
from load.load_scd2 import scd2_upsert
from logs.logger import log_run_start, log_run_end


pipeline_name = "book_analytics_scd2"

run_id = log_run_start(pipeline_name)

try:
    data = extract_all()

    extract_rows = len(data["fact_table"])

    print("Extract columns:", data["fact_table"].columns.tolist())

    df = transform_book_analytics(data)
    transform_rows = len(df)

    scd_result = scd2_upsert(df)

    log_run_end(
        run_id=run_id,
        status="SUCCESS",
        extract_rows=extract_rows,
        transform_rows=transform_rows,
        scd_new=scd_result["new_count"],
        scd_changed=scd_result["changed_count"],
        scd_inserted=scd_result["inserted"]
    )

    print("ETL SUCCESS:", scd_result)


except Exception as e:
    log_run_end(
        run_id=run_id,
        status="FAILED",
        fail_stage="UNKNOWN",
        error_message=str(e)
    )

    print("ETL FAILED:", str(e))
    raise