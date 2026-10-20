"""Column-level cleaning rules (pure Spark expressions, easy to unit test).

Every function takes / returns a pyspark Column so rules compose and can be
tested on a tiny DataFrame without running the whole pipeline.
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import reference as ref
from .config import MIN_VALID_DATE

NULL_TOKENS = ["N/A", "NA", "NULL", "NONE", "NAN", "-"]

# Trailing legal-entity suffixes removed when building the company key.
_LEGAL_SUFFIX = (
    r"(\s+(INC|INCORPORATED|LLC|L L C|LLP|LP|LTD|LIMITED|CO|COMPANY|CORP|"
    r"CORPORATION|NA|N A|NATIONAL ASSOCIATION|FSB|PLC))+$"
)


def _map_literal(mapping: dict[str, str]) -> Column:
    return F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in mapping.items()))


# ----------------------------------------------------------------------------
# generic text helpers
# ----------------------------------------------------------------------------
def clean_text(c: Column) -> Column:
    """Trim, collapse internal whitespace, empty string -> NULL."""
    collapsed = F.trim(F.regexp_replace(c, r"\s+", " "))
    return F.when(collapsed == "", F.lit(None).cast("string")).otherwise(collapsed)


def clean_token(c: Column) -> Column:
    """clean_text + treat placeholders like 'N/A' as NULL."""
    t = clean_text(c)
    return F.when(F.upper(t).isin(*NULL_TOKENS), F.lit(None).cast("string")).otherwise(t)


# ----------------------------------------------------------------------------
# dates
# ----------------------------------------------------------------------------
def parse_date(c: Column) -> Column:
    """CSV exports use MM/DD/YYYY, the API uses ISO timestamps. Accept both."""
    t = F.trim(c)
    return F.coalesce(
        F.to_date(t, "M/d/yyyy"),
        F.to_date(F.substring(t, 1, 10), "yyyy-MM-dd"),
    )


# ----------------------------------------------------------------------------
# company
# ----------------------------------------------------------------------------
def company_key(c: Column) -> Column:
    t = F.upper(clean_text(c))
    t = F.regexp_replace(t, "&", " AND ")
    t = F.regexp_replace(t, r"[^A-Z0-9 ]", " ")
    t = F.trim(F.regexp_replace(t, r"\s+", " "))
    t = F.regexp_replace(t, r"^THE ", "")
    stripped = F.trim(F.regexp_replace(t, _LEGAL_SUFFIX, ""))
    result = F.when(stripped == "", t).otherwise(stripped)
    return F.when(result == "", F.lit(None).cast("string")).otherwise(result)


# ----------------------------------------------------------------------------
# geography
# ----------------------------------------------------------------------------
def state_code(c: Column) -> Column:
    """Valid 2-letter code, or full state name mapped to code, else NULL."""
    t = F.upper(clean_token(c))
    by_name = _map_literal(ref.STATE_NAME_TO_CODE)
    return F.when(t.isin(*ref.STATES.keys()), t).otherwise(by_name[t])


def zip_normalised(c: Column) -> Column:
    return F.upper(clean_token(c))


def zip_is_masked(z: Column) -> Column:
    return z.rlike(r"^\d{3}XX$")


def zip_is_valid(z: Column) -> Column:
    return z.rlike(r"^\d{3}XX$") | z.rlike(r"^\d{5}(-\d{4})?$")


def zip3(z: Column) -> Column:
    return F.when(zip_is_valid(z), F.substring(z, 1, 3))


# ----------------------------------------------------------------------------
# categorical normalisation
# ----------------------------------------------------------------------------
def product_clean(c: Column) -> Column:
    t = clean_token(c)
    return F.coalesce(_map_literal(ref.PRODUCT_MAP)[F.lower(t)], t)


def response_clean(c: Column) -> Column:
    t = clean_token(c)
    return F.coalesce(_map_literal(ref.RESPONSE_MAP)[F.lower(t)], t)


def submitted_via_clean(c: Column) -> Column:
    t = clean_token(c)
    mapped = _map_literal(ref.SUBMITTED_VIA_MAP)[F.lower(t)]
    return F.when(t.isNull(), F.lit(None).cast("string")).otherwise(F.coalesce(mapped, F.lit("Other")))


def consent_code(c: Column) -> Column:
    t = F.lower(clean_token(c))
    return F.coalesce(_map_literal(ref.CONSENT_MAP)[t], F.lit("unknown"))


def yes_no_to_bool(c: Column) -> Column:
    t = F.lower(clean_token(c))
    return F.when(t == "yes", F.lit(True)).when(t == "no", F.lit(False)).cast("boolean")


# ----------------------------------------------------------------------------
# the base transformation: bronze strings -> typed, normalised columns
# ----------------------------------------------------------------------------
SILVER_COLUMNS = [
    "complaint_id",
    "date_received",
    "received_year_month",
    "date_sent_to_company",
    "days_to_company",
    "date_order_valid",
    "product_raw",
    "product_clean",
    "sub_product",
    "issue",
    "sub_issue",
    "company_raw",
    "company_key",
    "company_name_clean",
    "company_canonical_key",
    "company_resolution_method",
    "state_raw",
    "state_code",
    "state_valid",
    "zip_raw",
    "zip3",
    "zip_masked",
    "zip_valid",
    "tags",
    "is_older_american",
    "is_servicemember",
    "consumer_consent_provided",
    "has_narrative",
    "narrative",
    "narrative_char_len",
    "narrative_word_count",
    "pii_mask_token_count",
    "submitted_via",
    "company_public_response",
    "company_response_to_consumer",
    "is_relief",
    "timely_response",
    "consumer_disputed",
    "dq_flags",
    "_batch_id",
    "_ingested_at",
    "_ingest_seq",
    "silver_updated_at",
]


def clean_base(bronze: DataFrame) -> DataFrame:
    """Everything except company resolution (needs a driver-side mapping step)."""
    consent = consent_code(F.col("consumer_consent_provided"))
    narrative_text = clean_text(F.col("consumer_complaint_narrative"))
    # Privacy rule: never keep narrative text unless consent allows it.
    narrative = F.when(consent.isin("not_provided", "withdrawn"), F.lit(None).cast("string")).otherwise(
        narrative_text
    )

    z = zip_normalised(F.col("zip_code"))
    resp = response_clean(F.col("company_response_to_consumer"))
    tags = clean_token(F.col("tags"))

    df = bronze.select(
        F.col("complaint_id").cast("string").alias("_complaint_id_raw"),
        F.col("date_received").alias("_date_received_raw"),
        F.col("date_sent_to_company").alias("_date_sent_raw"),
        F.col("product").alias("product_raw"),
        product_clean(F.col("product")).alias("product_clean"),
        clean_token(F.col("sub_product")).alias("sub_product"),
        clean_token(F.col("issue")).alias("issue"),
        clean_token(F.col("sub_issue")).alias("sub_issue"),
        clean_text(F.col("company")).alias("company_raw"),
        company_key(F.col("company")).alias("company_key"),
        F.col("state").alias("state_raw"),
        state_code(F.col("state")).alias("state_code"),
        clean_token(F.col("state")).alias("_state_present"),
        F.col("zip_code").alias("zip_raw"),
        zip3(z).alias("zip3"),
        zip_is_masked(z).alias("zip_masked"),
        zip_is_valid(z).alias("zip_valid"),
        z.alias("_zip_present"),
        tags.alias("tags"),
        F.coalesce(F.lower(tags).contains("older american"), F.lit(False)).alias("is_older_american"),
        F.coalesce(F.lower(tags).contains("servicemember"), F.lit(False)).alias("is_servicemember"),
        consent.alias("consumer_consent_provided"),
        narrative.alias("narrative"),
        submitted_via_clean(F.col("submitted_via")).alias("submitted_via"),
        clean_token(F.col("company_public_response")).alias("company_public_response"),
        resp.alias("company_response_to_consumer"),
        F.lower(resp).isin(
            "closed with monetary relief", "closed with non-monetary relief", "closed with relief"
        ).alias("is_relief"),
        yes_no_to_bool(F.col("timely_response")).alias("timely_response"),
        yes_no_to_bool(F.col("consumer_disputed")).alias("consumer_disputed"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
        F.col("_ingest_seq"),
    )

    df = (
        df.withColumn("complaint_id", F.col("_complaint_id_raw").cast("long"))
        .withColumn("date_received", parse_date(F.col("_date_received_raw")))
        .withColumn("date_sent_to_company", parse_date(F.col("_date_sent_raw")))
        .withColumn("received_year_month", F.date_format("date_received", "yyyy-MM"))
        .withColumn("days_to_company", F.datediff("date_sent_to_company", "date_received"))
        .withColumn(
            "date_order_valid",
            F.when(F.col("days_to_company").isNull(), F.lit(None).cast("boolean")).otherwise(
                F.col("days_to_company") >= 0
            ),
        )
        .withColumn("state_valid", F.when(F.col("_state_present").isNull(), F.lit(None)).otherwise(F.col("state_code").isNotNull()))
        .withColumn("has_narrative", F.col("narrative").isNotNull())
        .withColumn("narrative_char_len", F.coalesce(F.length("narrative"), F.lit(0)))
        .withColumn(
            "narrative_word_count",
            F.when(F.col("narrative").isNull(), F.lit(0)).otherwise(F.size(F.split("narrative", " "))),
        )
        .withColumn(
            "pii_mask_token_count",
            F.when(F.col("narrative").isNull(), F.lit(0)).otherwise(
                F.regexp_count(F.col("narrative"), F.lit(r"X{2,}"))
            ),
        )
    )
    return df


def add_dq_flags(df: DataFrame) -> DataFrame:
    """Human-readable list of every data-quality issue found on the row."""
    flags = F.array(
        F.when(F.col("_state_present").isNull(), F.lit("state_missing")),
        F.when(F.col("state_code").isNull() & F.col("_state_present").isNotNull(), F.lit("state_invalid")),
        F.when(F.col("_state_present").isNotNull() & (F.upper(F.col("state_raw")) != F.col("state_code")) & F.col("state_code").isNotNull(), F.lit("state_normalised")),
        F.when(F.col("_zip_present").isNull(), F.lit("zip_missing")),
        F.when(F.col("zip_masked"), F.lit("zip_masked")),
        F.when(F.col("_zip_present").isNotNull() & ~F.col("zip_valid"), F.lit("zip_invalid")),
        F.when(F.col("date_order_valid") == F.lit(False), F.lit("sent_before_received")),
        F.when(F.col("date_sent_to_company").isNull(), F.lit("date_sent_missing")),
        F.when(F.col("company_raw") != F.col("company_name_clean"), F.lit("company_renamed")),
        F.when(F.col("product_raw") != F.col("product_clean"), F.lit("product_remapped")),
        F.when(F.col("consumer_consent_provided") == "unknown", F.lit("consent_unknown")),
    )
    return df.withColumn("dq_flags", F.filter(flags, lambda x: x.isNotNull()))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Hard rules: rows that cannot be trusted at all go to quarantine."""
    reason = (
        F.when(F.col("complaint_id").isNull(), F.lit("missing_or_non_numeric_complaint_id"))
        .when(F.col("date_received").isNull(), F.lit("date_received_unparseable"))
        .when(F.col("date_received") > F.current_date(), F.lit("date_received_in_future"))
        .when(F.col("date_received") < F.lit(MIN_VALID_DATE).cast("date"), F.lit("date_received_before_cfpb_start"))
    )
    flagged = df.withColumn("quarantine_reason", reason)
    valid = flagged.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    bad = flagged.filter(F.col("quarantine_reason").isNotNull())
    return valid, bad
