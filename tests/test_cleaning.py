from datetime import date

from pyspark.sql import functions as F

from pipeline import cleaning as c
from pipeline.bronze import BRONZE_COLUMNS


def one(spark, values, expr_fn):
    """Apply a column function to a list of raw values, return results in order."""
    df = spark.createDataFrame([(i, v) for i, v in enumerate(values)], "i int, v string")
    rows = df.select("i", expr_fn(F.col("v")).alias("r")).orderBy("i").collect()
    return [r["r"] for r in rows]


def test_company_key(spark):
    out = one(
        spark,
        [
            "BANK OF AMERICA, NATIONAL ASSOCIATION",
            "Bank of America, N.A.",
            "Wells Fargo Bank, N.A.",
            "The Acme Co.",
            "Smith & Sons, LLC",
            "   ",
            "INC",
            None,
        ],
        c.company_key,
    )
    assert out == [
        "BANK OF AMERICA",
        "BANK OF AMERICA",
        "WELLS FARGO BANK",
        "ACME",
        "SMITH AND SONS",
        None,
        "INC",  # never reduce a name to nothing
        None,
    ]


def test_state_code(spark):
    out = one(spark, ["TX", "tx", " ca ", "Texas", "new york", "XX", "N/A", "", None, "dc", "PR"], c.state_code)
    assert out == ["TX", "TX", "CA", "TX", "NY", None, None, None, None, "DC", "PR"]


def test_zip_rules(spark):
    z = lambda col: c.zip_normalised(col)  # noqa: E731
    assert one(spark, ["303XX", "30303", "30303-1234", "ABCDE", "", "30", "303xx"], lambda col: c.zip_is_valid(z(col))) == [
        True, True, True, False, None, False, True,
    ]
    assert one(spark, ["303XX", "30303", "30303-1234", "ABCDE"], lambda col: c.zip3(z(col))) == ["303", "303", "303", None]
    assert one(spark, ["303XX", "30303"], lambda col: c.zip_is_masked(z(col))) == [True, False]


def test_parse_date(spark):
    out = one(
        spark,
        ["03/05/2021", "3/5/2021", "2021-03-05", "2021-03-05T12:00:00-05:00", "13/45/2022", "", None, "garbage"],
        c.parse_date,
    )
    d = date(2021, 3, 5)
    assert out == [d, d, d, d, None, None, None, None]


def test_product_clean(spark):
    cr = "Credit reporting or other personal consumer reports"
    out = one(
        spark,
        ["Credit reporting", "other personal consumer report", " DEBT COLLECTION ", "Prepaid card", "Mystery product", "", None],
        c.product_clean,
    )
    assert out == [cr, cr, "Debt collection", "Credit card or prepaid card", "Mystery product", None, None]


def test_consent_and_bools(spark):
    assert one(spark, ["Consent provided", "consent not provided", "Consent withdrawn", "N/A", "", None, "weird"], c.consent_code) == [
        "provided", "not_provided", "withdrawn", "unknown", "unknown", "unknown", "unknown",
    ]
    assert one(spark, ["Yes", "no", " YES ", "N/A", None], c.yes_no_to_bool) == [True, False, True, None, None]


def test_submitted_via_and_response(spark):
    assert one(spark, ["web", "WEB", "Postal mail", "carrier pigeon", None], c.submitted_via_clean) == [
        "Web", "Web", "Postal mail", "Other", None,
    ]
    assert one(spark, ["closed with explanation ", "CLOSED WITHOUT RELIEF", "Something new"], c.response_clean) == [
        "Closed with explanation", "Closed without relief", "Something new",
    ]


def _bronze_df(spark, rows):
    schema = ", ".join(f"{col} string" for col in BRONZE_COLUMNS)
    df = spark.createDataFrame([tuple(r.get(col) for col in BRONZE_COLUMNS) for r in rows], schema)
    return (
        df.withColumn("_batch_id", F.lit("b1"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_seq", F.monotonically_increasing_id())
    )


def test_clean_base_end_to_end(spark):
    rows = [
        {
            "complaint_id": "42", "date_received": "03/05/2021", "date_sent_to_company": "03/04/2021",
            "product": "Credit card", "company": "Capital One, N.A.", "state": "texas", "zip_code": "750XX",
            "consumer_consent_provided": "Consent provided",
            "consumer_complaint_narrative": "On XX/XX/XXXX  my card ended in XXXX\n was charged.",
            "timely_response": "Yes", "consumer_disputed": "N/A", "tags": "Older American, Servicemember",
            "company_response_to_consumer": "closed with monetary relief",
        },
        {
            "complaint_id": "43", "date_received": "2021-03-06", "date_sent_to_company": "2021-03-06",
            "consumer_consent_provided": "Consent not provided",
            "consumer_complaint_narrative": "this must be dropped", "state": "ZZ",
        },
    ]
    out = {r["complaint_id"]: r for r in c.clean_base(_bronze_df(spark, rows)).collect()}

    a = out[42]
    assert a["date_received"] == date(2021, 3, 5)
    assert a["received_year_month"] == "2021-03"
    assert a["days_to_company"] == -1 and a["date_order_valid"] is False
    assert a["product_clean"] == "Credit card or prepaid card"
    assert a["company_key"] == "CAPITAL ONE"
    assert a["state_code"] == "TX" and a["state_valid"] is True
    assert a["zip3"] == "750" and a["zip_masked"] is True
    assert a["has_narrative"] is True
    assert a["narrative"] == "On XX/XX/XXXX my card ended in XXXX was charged."
    assert a["pii_mask_token_count"] == 4  # XX / XX / XXXX in the date + XXXX card digits
    assert a["narrative_word_count"] == 9
    assert a["is_relief"] is True and a["timely_response"] is True and a["consumer_disputed"] is None
    assert a["is_older_american"] is True and a["is_servicemember"] is True

    b = out[43]
    assert b["narrative"] is None and b["has_narrative"] is False  # privacy rule
    assert b["state_code"] is None and b["state_valid"] is False


def test_hard_rules_quarantine(spark):
    rows = [
        {"complaint_id": "1", "date_received": "01/05/2021"},
        {"complaint_id": "", "date_received": "01/05/2021"},
        {"complaint_id": "3", "date_received": "13/45/2021"},
        {"complaint_id": "4", "date_received": "2099-01-01"},
        {"complaint_id": "5", "date_received": "01/01/2005"},
        {"complaint_id": "abc", "date_received": "01/05/2021"},
    ]
    df = c.clean_base(_bronze_df(spark, rows))
    df = df.withColumn("company_name_clean", F.lit(None).cast("string"))
    df = c.add_dq_flags(df)
    valid, bad = c.split_valid_quarantine(df)
    assert [r["complaint_id"] for r in valid.collect()] == [1]
    reasons = sorted(r["quarantine_reason"] for r in bad.collect())
    assert reasons == sorted(
        [
            "missing_or_non_numeric_complaint_id",
            "missing_or_non_numeric_complaint_id",
            "date_received_unparseable",
            "date_received_in_future",
            "date_received_before_cfpb_start",
        ]
    )
