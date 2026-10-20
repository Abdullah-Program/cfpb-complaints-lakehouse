from pipeline.company_resolution import build_company_map


def obs(*pairs):
    return [(k, d, n) for k, d, n in pairs]


def test_rules_and_typos_roll_up_to_one_company():
    m = build_company_map(
        obs(
            ("BANK OF AMERICA", "BANK OF AMERICA, NATIONAL ASSOCIATION", 500),
            ("BANK OF AMERICA NATIONAL ASSOCIATION BANK", "x", 3),  # rule via prefix
            ("BANK OF AMERCIA", "BANK OF AMERCIA, N.A.", 4),  # typo -> fuzzy
        )
    )
    names = {v.canonical_name for v in m.values()}
    assert names == {"Bank of America"}
    assert m["BANK OF AMERICA"].method == "rule"
    assert m["BANK OF AMERCIA"].method == "fuzzy"


def test_fuzzy_merges_typo_into_most_frequent_spelling():
    m = build_company_map(
        obs(
            ("PIONEER COLLECTIONS", "Pioneer Collections, LLC", 100),
            ("PIONEER COLLECTOINS", "PIONEER COLLECTOINS", 2),
        )
    )
    assert m["PIONEER COLLECTOINS"].canonical_key == "PIONEER COLLECTIONS"
    assert m["PIONEER COLLECTOINS"].canonical_name == "Pioneer Collections, LLC"


def test_different_companies_are_not_merged():
    m = build_company_map(
        obs(
            ("FIRST NATIONAL BANK OF OMAHA", "a", 50),
            ("FIRST NATIONAL BANK OF TEXAS", "b", 40),
            ("SUMMIT AUTO FINANCE", "c", 30),
            ("SUMMIT HOME FINANCE", "d", 30),
        )
    )
    assert len({v.canonical_key for v in m.values()}) == 4


def test_numbers_block_merges():
    m = build_company_map(obs(("ACME HOLDINGS 1", "a", 10), ("ACME HOLDINGS 2", "b", 9)))
    assert m["ACME HOLDINGS 1"].canonical_key != m["ACME HOLDINGS 2"].canonical_key


def test_existing_mapping_is_stable_across_runs():
    first = build_company_map(obs(("SILVERLINE FUNDING", "Silverline Funding LLC", 10)))
    # a later batch brings a typo variant AND a much more frequent spelling
    second = build_company_map(
        obs(("SILVERLNIE FUNDING", "SILVERLNIE FUNDING", 99), ("SILVERLINE FUNDING", "x", 500)), existing=first
    )
    assert second["SILVERLINE FUNDING"] == first["SILVERLINE FUNDING"]  # never re-mapped
    assert second["SILVERLNIE FUNDING"].canonical_key == "SILVERLINE FUNDING"
