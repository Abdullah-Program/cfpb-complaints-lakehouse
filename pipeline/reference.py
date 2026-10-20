"""Reference data used by the cleaning layer (single source of truth).

The dbt seeds in dbt_cfpb/seeds/ are generated from these structures with
`python -m pipeline.reference --write-seeds`, so Spark and dbt never disagree.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

# code -> (name, census region / group)
STATES: dict[str, tuple[str, str]] = {
    "AL": ("Alabama", "South"),
    "AK": ("Alaska", "West"),
    "AZ": ("Arizona", "West"),
    "AR": ("Arkansas", "South"),
    "CA": ("California", "West"),
    "CO": ("Colorado", "West"),
    "CT": ("Connecticut", "Northeast"),
    "DE": ("Delaware", "South"),
    "DC": ("District of Columbia", "South"),
    "FL": ("Florida", "South"),
    "GA": ("Georgia", "South"),
    "HI": ("Hawaii", "West"),
    "ID": ("Idaho", "West"),
    "IL": ("Illinois", "Midwest"),
    "IN": ("Indiana", "Midwest"),
    "IA": ("Iowa", "Midwest"),
    "KS": ("Kansas", "Midwest"),
    "KY": ("Kentucky", "South"),
    "LA": ("Louisiana", "South"),
    "ME": ("Maine", "Northeast"),
    "MD": ("Maryland", "South"),
    "MA": ("Massachusetts", "Northeast"),
    "MI": ("Michigan", "Midwest"),
    "MN": ("Minnesota", "Midwest"),
    "MS": ("Mississippi", "South"),
    "MO": ("Missouri", "Midwest"),
    "MT": ("Montana", "West"),
    "NE": ("Nebraska", "Midwest"),
    "NV": ("Nevada", "West"),
    "NH": ("New Hampshire", "Northeast"),
    "NJ": ("New Jersey", "Northeast"),
    "NM": ("New Mexico", "West"),
    "NY": ("New York", "Northeast"),
    "NC": ("North Carolina", "South"),
    "ND": ("North Dakota", "Midwest"),
    "OH": ("Ohio", "Midwest"),
    "OK": ("Oklahoma", "South"),
    "OR": ("Oregon", "West"),
    "PA": ("Pennsylvania", "Northeast"),
    "RI": ("Rhode Island", "Northeast"),
    "SC": ("South Carolina", "South"),
    "SD": ("South Dakota", "Midwest"),
    "TN": ("Tennessee", "South"),
    "TX": ("Texas", "South"),
    "UT": ("Utah", "West"),
    "VT": ("Vermont", "Northeast"),
    "VA": ("Virginia", "South"),
    "WA": ("Washington", "West"),
    "WV": ("West Virginia", "South"),
    "WI": ("Wisconsin", "Midwest"),
    "WY": ("Wyoming", "West"),
    # Territories / freely associated states
    "PR": ("Puerto Rico", "Territory"),
    "VI": ("U.S. Virgin Islands", "Territory"),
    "GU": ("Guam", "Territory"),
    "AS": ("American Samoa", "Territory"),
    "MP": ("Northern Mariana Islands", "Territory"),
    "UM": ("U.S. Minor Outlying Islands", "Territory"),
    "FM": ("Federated States of Micronesia", "Territory"),
    "MH": ("Marshall Islands", "Territory"),
    "PW": ("Palau", "Territory"),
    # Military mail codes
    "AA": ("Armed Forces Americas", "Military"),
    "AE": ("Armed Forces Europe", "Military"),
    "AP": ("Armed Forces Pacific", "Military"),
}

STATE_NAME_TO_CODE: dict[str, str] = {name.upper(): code for code, (name, _) in STATES.items()}

# Product taxonomy drift: the CFPB renamed / merged products over time.
# key = lower-cased raw value, value = canonical label.
# NOTE: review against the current field reference
# (https://cfpb.github.io/api/ccdb/fields.html) when you refresh the data.
CREDIT_REPORTING = "Credit reporting or other personal consumer reports"
CARDS = "Credit card or prepaid card"
DEPOSIT = "Checking or savings account"
PAYDAY = "Payday loan, title loan, personal loan, or advance loan"
MONEY_TRANSFER = "Money transfer, virtual currency, or money service"

PRODUCT_MAP: dict[str, str] = {
    "credit reporting": CREDIT_REPORTING,
    "other personal consumer report": CREDIT_REPORTING,
    "credit reporting, credit repair services, or other personal consumer reports": CREDIT_REPORTING,
    "credit reporting or other personal consumer reports": CREDIT_REPORTING,
    "credit card": CARDS,
    "prepaid card": CARDS,
    "credit card or prepaid card": CARDS,
    "bank account or service": DEPOSIT,
    "checking or savings account": DEPOSIT,
    "payday loan": PAYDAY,
    "payday loan, title loan, or personal loan": PAYDAY,
    "payday loan, title loan, personal loan, or advance loan": PAYDAY,
    "money transfers": MONEY_TRANSFER,
    "virtual currency": MONEY_TRANSFER,
    "money transfer, virtual currency, or money service": MONEY_TRANSFER,
}

# canonical product -> analytical group (used for the dbt seed)
PRODUCT_GROUPS: dict[str, str] = {
    CREDIT_REPORTING: "Credit reporting",
    "Debt collection": "Debt collection",
    "Debt or credit management": "Debt collection",
    "Mortgage": "Mortgage",
    CARDS: "Cards",
    DEPOSIT: "Deposit accounts",
    "Student loan": "Loans",
    "Vehicle loan or lease": "Loans",
    "Consumer Loan": "Loans",
    PAYDAY: "Loans",
    MONEY_TRANSFER: "Money transfer",
    "Other financial service": "Other",
}

# Canonical labels must also match case-insensitively ("DEBT COLLECTION" -> "Debt collection").
for _canonical in PRODUCT_GROUPS:
    PRODUCT_MAP.setdefault(_canonical.lower(), _canonical)

RESPONSE_MAP: dict[str, str] = {
    "closed with explanation": "Closed with explanation",
    "closed with monetary relief": "Closed with monetary relief",
    "closed with non-monetary relief": "Closed with non-monetary relief",
    "closed without relief": "Closed without relief",
    "closed with relief": "Closed with relief",
    "closed": "Closed",
    "in progress": "In progress",
    "untimely response": "Untimely response",
}

SUBMITTED_VIA_MAP: dict[str, str] = {
    "web": "Web",
    "phone": "Phone",
    "referral": "Referral",
    "postal mail": "Postal mail",
    "email": "Email",
    "fax": "Fax",
}

CONSENT_MAP: dict[str, str] = {
    "consent provided": "provided",
    "consent not provided": "not_provided",
    "consent withdrawn": "withdrawn",
    "other": "other",
}

# Parent-company roll-up rules, applied to the NORMALISED company key
# (upper-case, punctuation stripped, legal suffixes removed).
COMPANY_RULES: list[tuple[str, str]] = [
    (r"^BANK OF AMERICA\b", "Bank of America"),
    (r"^WELLS FARGO\b", "Wells Fargo"),
    (r"^(JPMORGAN|JP MORGAN)\b|^CHASE BANK\b", "JPMorgan Chase"),
    (r"^CITIBANK\b|^CITI BANK\b|^CITIGROUP\b", "Citibank"),
    (r"^CAPITAL ONE\b", "Capital One"),
    (r"^EQUIFAX\b", "Equifax"),
    (r"^EXPERIAN\b", "Experian"),
    (r"^TRANS ?UNION\b", "TransUnion"),
    (r"^NAVIENT\b", "Navient"),
    (r"^SYNCHRONY\b", "Synchrony Financial"),
    (r"^DISCOVER\b", "Discover"),
    (r"^AMERICAN EXPRESS\b|^AMEX\b", "American Express"),
    (r"^(U S|US) (BANCORP|BANK)\b", "U.S. Bancorp"),
    (r"^PNC\b", "PNC"),
    (r"^TD BANK\b", "TD Bank"),
    (r"^ALLY\b", "Ally Financial"),
    (r"^TRUIST\b", "Truist"),
]

BIG_COMPANY_CATEGORY: dict[str, str] = {
    "Bank of America": "Large bank",
    "Wells Fargo": "Large bank",
    "JPMorgan Chase": "Large bank",
    "Citibank": "Large bank",
    "U.S. Bancorp": "Large bank",
    "PNC": "Large bank",
    "TD Bank": "Large bank",
    "Truist": "Large bank",
    "Capital One": "Card issuer / lender",
    "Synchrony Financial": "Card issuer / lender",
    "Discover": "Card issuer / lender",
    "American Express": "Card issuer / lender",
    "Ally Financial": "Card issuer / lender",
    "Navient": "Student loan servicer",
    "Equifax": "Credit bureau",
    "Experian": "Credit bureau",
    "TransUnion": "Credit bureau",
}


def write_seeds(seed_dir: Path) -> None:
    seed_dir.mkdir(parents=True, exist_ok=True)

    with open(seed_dir / "state_codes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["state_code", "state_name", "region"])
        for code, (name, region) in sorted(STATES.items()):
            w.writerow([code, name, region])

    with open(seed_dir / "product_taxonomy.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_clean", "product_group"])
        for product, group in PRODUCT_GROUPS.items():
            w.writerow([product, group])

    with open(seed_dir / "company_category.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["company_name_clean", "company_category"])
        for company, cat in BIG_COMPANY_CATEGORY.items():
            w.writerow([company, cat])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--write-seeds", action="store_true")
    ap.add_argument("--seed-dir", default="dbt_cfpb/seeds")
    args = ap.parse_args()
    if args.write_seeds:
        write_seeds(Path(args.seed_dir))
        print(f"seeds written to {args.seed_dir}")
