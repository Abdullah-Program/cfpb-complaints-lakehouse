"""Synthetic, deliberately messy CFPB-shaped data for local dev, tests and CI.

!! This is SYNTHETIC data. Company names are real-world names used only so the
!! entity-resolution rules have something realistic to chew on; the complaints,
!! counts and narratives are invented and mean nothing.

Injected problems (so the cleaning layer has real work to do):
  company-name variants + typos, legacy product names, lowercase / full-name /
  invalid states, masked / malformed ZIPs, mixed date formats, unparseable and
  future dates, sent-before-received dates, N/A + blank placeholders, exact
  duplicate rows, missing complaint ids, inconsistent casing/whitespace in
  categorical columns, narratives present without consent.

    python -m pipeline.generate_sample_data --rows 20000 --out data/raw/sample/initial.csv
    python -m pipeline.generate_sample_data --delta --base data/raw/sample/initial.csv \
        --out data/raw/sample/delta_1.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .reference import STATES

CSV_COLUMNS = [
    "Date received",
    "Product",
    "Sub-product",
    "Issue",
    "Sub-issue",
    "Consumer complaint narrative",
    "Company public response",
    "Company",
    "State",
    "ZIP code",
    "Tags",
    "Consumer consent provided?",
    "Submitted via",
    "Date sent to company",
    "Company response to consumer",
    "Timely response?",
    "Consumer disputed?",
    "Complaint ID",
]

BIG_COMPANIES = {
    # canonical -> raw spellings seen in the wild (and a typo or two)
    "Bank of America": [
        "BANK OF AMERICA, NATIONAL ASSOCIATION",
        "Bank of America",
        "BANK OF AMERICA NA",
        "Bank of America, N.A.",
        "BANK OF AMERCIA, NATIONAL ASSOCIATION",
    ],
    "Wells Fargo": ["WELLS FARGO & COMPANY", "Wells Fargo Bank, N.A.", "WELLS FARGO BANK NA", "wells fargo"],
    "JPMorgan Chase": ["JPMORGAN CHASE & CO.", "JPMorgan Chase Bank, N.A.", "JP MORGAN CHASE", "CHASE BANK USA, N.A."],
    "Citibank": ["CITIBANK, N.A.", "Citibank", "CITIGROUP INC.", "Citi Bank NA"],
    "Capital One": ["CAPITAL ONE FINANCIAL CORPORATION", "Capital One Bank (USA), N.A.", "CAPITAL ONE", "Capital One Auto Finance"],
    "Equifax": ["EQUIFAX, INC.", "Equifax Information Services LLC", "EQUIFAX"],
    "Experian": ["Experian Information Solutions Inc.", "EXPERIAN", "Experian Information Solutions, Inc"],
    "TransUnion": ["TRANSUNION INTERMEDIATE HOLDINGS, INC.", "Trans Union LLC", "TransUnion"],
    "Navient": ["Navient Solutions, LLC.", "NAVIENT CORPORATION", "Navient"],
    "Synchrony Financial": ["SYNCHRONY FINANCIAL", "Synchrony Bank", "SYNCHRONY BANK"],
    "Discover": ["DISCOVER BANK", "Discover Financial Services", "DISCOVER"],
    "American Express": ["AMERICAN EXPRESS COMPANY", "American Express National Bank", "AMEX"],
}
SMALL_COMPANY_STEMS = [
    "Acme Credit Solutions", "Northstar Recovery", "Blue Harbor Mortgage", "Keystone Lending",
    "Pioneer Collections", "Summit Auto Finance", "Lakeside Servicing", "Granite Debt Partners",
    "Maple Leaf Credit Union", "Redwood Financial", "Evergreen Student Loans", "Cobalt Payments",
    "Silverline Funding", "Harborview Bank", "Ironclad Collections", "Beacon Money Transfer",
]
SUFFIXES = ["", " Inc.", ", LLC", " Corp", " LLC", ", Inc", " Co."]

PRODUCTS = {
    "Credit reporting or other personal consumer reports": [
        "Credit reporting", "Other personal consumer report",
        "Credit reporting, credit repair services, or other personal consumer reports",
        "Credit reporting or other personal consumer reports",
    ],
    "Debt collection": ["Debt collection"],
    "Mortgage": ["Mortgage"],
    "Credit card or prepaid card": ["Credit card", "Prepaid card", "Credit card or prepaid card"],
    "Checking or savings account": ["Bank account or service", "Checking or savings account"],
    "Student loan": ["Student loan"],
    "Vehicle loan or lease": ["Vehicle loan or lease"],
    "Money transfer, virtual currency, or money service": ["Money transfers", "Virtual currency", "Money transfer, virtual currency, or money service"],
    "Payday loan, title loan, personal loan, or advance loan": ["Payday loan", "Payday loan, title loan, or personal loan"],
}
PRODUCT_WEIGHTS = [0.38, 0.14, 0.09, 0.1, 0.08, 0.05, 0.05, 0.05, 0.06]
SUB_PRODUCTS = {
    "Credit reporting or other personal consumer reports": ["Credit reporting", "Other personal consumer report"],
    "Debt collection": ["I do not know", "Credit card debt", "Medical debt", "Payday loan debt"],
    "Mortgage": ["Conventional home mortgage", "FHA mortgage", "VA mortgage"],
    "Credit card or prepaid card": ["General-purpose credit card or charge card", "Store credit card"],
    "Checking or savings account": ["Checking account", "Savings account"],
}
ISSUES = {
    "Credit reporting or other personal consumer reports": [
        "Incorrect information on your report", "Problem with a credit reporting company's investigation into an existing problem",
        "Improper use of your report",
    ],
    "Debt collection": ["Attempts to collect debt not owed", "Communication tactics", "Took or threatened to take negative or legal action"],
    "Mortgage": ["Trouble during payment process", "Struggling to pay mortgage", "Closing on a mortgage"],
    "Credit card or prepaid card": ["Problem with a purchase shown on your statement", "Fraud or scam", "Billing disputes"],
    "Checking or savings account": ["Managing an account", "Problem with a lender or other company charging your account", "Opening an account"],
    "Student loan": ["Dealing with your lender or servicer", "Struggling to repay your loan"],
    "Vehicle loan or lease": ["Managing the loan or lease", "Problems at the end of the loan or lease"],
    "Money transfer, virtual currency, or money service": ["Fraud or scam", "Other transaction problem"],
    "Payday loan, title loan, personal loan, or advance loan": ["Charged fees or interest you didn't expect", "Can't contact lender"],
}
RESPONSES = ["Closed with explanation", "Closed with non-monetary relief", "Closed with monetary relief", "Closed without relief", "In progress", "Untimely response"]
RESPONSE_P = [0.72, 0.1, 0.06, 0.07, 0.03, 0.02]
PUBLIC_RESPONSES = [
    "Company has responded to the consumer and the CFPB and chooses not to provide a public response",
    "Company believes it acted appropriately as authorized by contract or law",
    "Company disputes the facts presented in the complaint", "",
]
NARRATIVE_TEMPLATES = [
    "On XX/XX/XXXX I noticed an account on my report that I do not recognize. I called XXXX twice and was told it would be fixed.",
    "I have been charged {amt} in fees I did not authorize. My account number ends in XXXX. Please help.",
    "A collector keeps calling me about a debt I do not owe.\nI sent a dispute letter on XX/XX/2023 and never got a reply.",
    "The servicer misapplied my last two payments and now reports me late. This is hurting my credit.",
    "Fraudulent transaction of {amt} appeared on my statement. The company refused to refund me.",
]
STATE_CODES = list(STATES)
TOP_STATES = ["CA", "TX", "FL", "NY", "GA", "PA", "IL", "OH", "NC", "MI", "NJ", "VA", "AZ", "MD", "TN"]


def _typo(rng: np.random.Generator, s: str) -> str:
    if len(s) < 6:
        return s
    i = int(rng.integers(1, len(s) - 2))
    return s[:i] + s[i + 1] + s[i] + s[i + 2:]


def _company_pool(rng: np.random.Generator) -> tuple[list[str], np.ndarray]:
    names: list[str] = []
    weights: list[float] = []
    for variants in BIG_COMPANIES.values():
        for i, v in enumerate(variants):
            names.append(v)
            weights.append(8.0 / (1 + i * 0.6))
    for stem in SMALL_COMPANY_STEMS:
        for _ in range(3):
            variant = stem + SUFFIXES[int(rng.integers(0, len(SUFFIXES)))]
            variant = variant.upper() if rng.random() < 0.4 else variant
            names.append(variant)
            weights.append(0.6)
            if rng.random() < 0.3:
                names.append(_typo(rng, variant))
                weights.append(0.1)
    w = np.array(weights)
    return names, w / w.sum()


def _fmt_date(rng: np.random.Generator, d: pd.Timestamp) -> str:
    r = rng.random()
    if r < 0.003:
        return "13/45/2022"
    if r < 0.15:
        return d.strftime("%Y-%m-%d")
    return d.strftime("%m/%d/%Y")


def _noise_case(rng: np.random.Generator, s: str) -> str:
    r = rng.random()
    if r < 0.04:
        return s.lower()
    if r < 0.07:
        return s.upper()
    if r < 0.10:
        return f" {s} "
    return s


def generate_rows(rng: np.random.Generator, n: int, id_start: int, start: str, end: str) -> pd.DataFrame:
    companies, c_p = _company_pool(rng)
    products = list(PRODUCTS)
    days = pd.date_range(start, end, freq="D")
    received = pd.to_datetime(rng.choice(days, size=n))
    received = received.sort_values()

    recs = []
    for i in range(n):
        d = received[i]
        prod = products[int(rng.choice(len(products), p=PRODUCT_WEIGHTS))]
        raw_prod = PRODUCTS[prod][int(rng.integers(0, len(PRODUCTS[prod])))]
        sub_pool = SUB_PRODUCTS.get(prod, [""])
        sub = sub_pool[int(rng.integers(0, len(sub_pool)))]
        issue = ISSUES[prod][int(rng.integers(0, len(ISSUES[prod])))]

        # state
        r = rng.random()
        if r < 0.90:
            st = TOP_STATES[int(rng.integers(0, len(TOP_STATES)))] if rng.random() < 0.6 else STATE_CODES[int(rng.integers(0, len(STATE_CODES)))]
        elif r < 0.93:
            st = STATE_CODES[int(rng.integers(0, len(STATE_CODES)))].lower()
        elif r < 0.95:
            st = STATES[STATE_CODES[int(rng.integers(0, len(STATE_CODES)))]][0]
        elif r < 0.97:
            st = str(rng.choice(["XX", "ZZ", "00", "UNK"]))
        else:
            st = str(rng.choice(["", "N/A"]))

        # zip
        r = rng.random()
        z3 = f"{int(rng.integers(100, 999))}"
        if r < 0.60:
            zp = z3 + "XX"
        elif r < 0.90:
            zp = z3 + f"{int(rng.integers(10, 99))}"
        elif r < 0.95:
            zp = ""
        elif r < 0.98:
            zp = z3 + f"{int(rng.integers(10, 99))}-{int(rng.integers(1000, 9999))}"
        else:
            zp = str(rng.choice(["ABCDE", "0000", "N/A"]))

        # consent + narrative
        r = rng.random()
        if r < 0.30:
            consent, narrative = "Consent provided", NARRATIVE_TEMPLATES[int(rng.integers(0, len(NARRATIVE_TEMPLATES)))].format(amt=f"${int(rng.integers(20, 900))}")
        elif r < 0.70:
            consent, narrative = "Consent not provided", ""
            if rng.random() < 0.02:
                narrative = "leaked narrative that should be dropped (consent not provided)"
        elif r < 0.90:
            consent, narrative = "N/A", ""
        elif r < 0.95:
            consent, narrative = "Other", ""
        else:
            consent, narrative = "Consent withdrawn", ""

        # dates
        sent = d + pd.Timedelta(days=int(rng.integers(0, 5)))
        if rng.random() < 0.005:
            sent = d - pd.Timedelta(days=int(rng.integers(1, 4)))
        recv_s = _fmt_date(rng, d)
        if rng.random() < 0.002:
            recv_s = "2030-01-15"
        sent_s = _fmt_date(rng, sent)

        resp = RESPONSES[int(rng.choice(len(RESPONSES), p=RESPONSE_P))]
        timely = "Yes" if rng.random() < 0.97 else "No"
        disputed = str(rng.choice(["N/A", "N/A", "No", "Yes"], p=[0.5, 0.2, 0.2, 0.1]))
        via = str(rng.choice(["Web", "Phone", "Referral", "Postal mail", "Email", "Fax", "web", "WEB"], p=[0.5, 0.1, 0.12, 0.05, 0.05, 0.03, 0.1, 0.05]))
        tags = str(rng.choice(["", "Older American", "Servicemember", "Older American, Servicemember"], p=[0.8, 0.12, 0.06, 0.02]))

        cid = "" if rng.random() < 0.003 else str(id_start + i)
        recs.append(
            {
                "Date received": recv_s,
                "Product": _noise_case(rng, raw_prod),
                "Sub-product": sub,
                "Issue": issue,
                "Sub-issue": "",
                "Consumer complaint narrative": narrative,
                "Company public response": PUBLIC_RESPONSES[int(rng.integers(0, len(PUBLIC_RESPONSES)))],
                "Company": companies[int(rng.choice(len(companies), p=c_p))],
                "State": st,
                "ZIP code": zp,
                "Tags": tags,
                "Consumer consent provided?": consent,
                "Submitted via": via,
                "Date sent to company": sent_s,
                "Company response to consumer": _noise_case(rng, resp),
                "Timely response?": timely,
                "Consumer disputed?": disputed,
                "Complaint ID": cid,
            }
        )
    return pd.DataFrame.from_records(recs, columns=CSV_COLUMNS)


def make_initial(rows: int, seed: int, out: Path) -> None:
    rng = np.random.default_rng(seed)
    df = generate_rows(rng, rows, id_start=1_000_000, start="2021-01-01", end="2025-06-30")
    dups = df.sample(frac=0.015, random_state=seed)  # exact duplicate rows
    df = pd.concat([df, dups], ignore_index=True).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"wrote {len(df)} rows -> {out}")


def make_delta(base: Path, seed: int, out: Path, new_rows: int = 800, updates: int = 300, late: int = 150) -> None:
    rng = np.random.default_rng(seed)
    base_df = pd.read_csv(base, dtype=str, keep_default_na=False)
    base_ids = base_df[base_df["Complaint ID"] != ""]
    max_id = int(base_ids["Complaint ID"].astype(int).max())

    fresh = generate_rows(rng, new_rows, id_start=max_id + 1, start="2025-07-01", end="2025-07-14")
    late_df = generate_rows(rng, late, id_start=max_id + 1 + new_rows, start="2025-03-01", end="2025-03-31")

    # corrections: same complaint_id, company response / dispute changed later
    upd = base_ids.drop_duplicates("Complaint ID").sample(n=min(updates, len(base_ids)), random_state=seed).copy()
    upd["Company response to consumer"] = "Closed with monetary relief"
    upd["Consumer disputed?"] = "Yes"
    upd["Timely response?"] = "Yes"

    df = pd.concat([fresh, late_df, upd], ignore_index=True).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"wrote delta: {new_rows} new + {late} late-arriving + {len(upd)} corrections -> {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw/sample/initial.csv")
    ap.add_argument("--delta", action="store_true", help="generate an incremental delta file")
    ap.add_argument("--base", default="data/raw/sample/initial.csv", help="initial file (delta only)")
    args = ap.parse_args()
    if args.delta:
        make_delta(Path(args.base), args.seed + 1, Path(args.out))
    else:
        make_initial(args.rows, args.seed, Path(args.out))


if __name__ == "__main__":
    main()
