# =============================================================================
# BEA Detail Industry – NAICS – HTS-10 Concordance
#
# Purpose:  Build a three-way crosswalk mapping BEA Detail industry codes to
#           HTS-10 codes via 2017 NAICS, for use in merging USITC import trade
#           data onto BEA Detail industries.
#
# Inputs:   Margins_Before_Redefinitions_Detail.xlsx  (sheet: "NAICS Codes")
#           commodity_translation_wizard.xlsx          (sheet: "Import Concordance")
#
# Output:   bea_naics_hts10_concordance.rds / .csv
#
# Coverage notes:
#   - 213 of 390 BEA Detail codes match to at least one HTS-10 code.
#   - 177 unmatched codes are predominantly services industries (4xx–8xx),
#     which have no HTS representation by construction.
#   - ~40 goods-sector codes (agriculture, basic materials, some manufacturing)
#     are absent from the USITC import concordance; treated as a source
#     coverage gap and dropped.
#   - 31511X USITC NAICS codes (22 rows) have no BEA match at any prefix
#     depth and are dropped.
#   - Match type hierarchy: exact_6d > ambiguous_prefix > fallback_5d.
#     Where the same bea_detail/hts10 pair arises via multiple paths, the
#     highest-confidence match is retained.
#   - 1,009 HTS-10 codes map to more than one BEA industry (mostly 2-way);
#     deduplication via match_rank handles within-concordance fan-out, but
#     downstream aggregation of trade values must account for any remaining
#     many-to-many structure.
# =============================================================================

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)


# ── 1. Load and parse BEA NAICS crosswalk ────────────────────────────────────

raw_bea <- read_excel(
  "Margins_Before_Redefinitions_Detail.xlsx",
  sheet     = "NAICS Codes",
  skip      = 4,
  col_names = TRUE
)

bea <- raw_bea |>
  rename(
    bea_detail = 4,   # Column D: BEA Detail code
    naics_raw  = 7    # Column G: Related 2017 NAICS Codes
  ) |>
  select(bea_detail, naics_raw) |>
  filter(
    !is.na(bea_detail),
    !is.na(naics_raw),
    str_trim(naics_raw) != "n.a."
  )

# Expand BEA range notation: "11113-6" → c("11113","11114","11115","11116")
# The suffix replaces the trailing n digits of the base code, where n = nchar(suffix).
# Purely numeric tokens only; alphanumeric codes (e.g. 112A00) pass through unchanged.
expand_bea_range <- function(token) {
  if (str_detect(token, "^[0-9]+-[0-9]+$")) {
    parts  <- str_split(token, "-")[[1]]
    base   <- parts[1]
    suffix <- parts[2]
    n      <- nchar(suffix)
    prefix <- str_sub(base, 1, nchar(base) - n)
    start  <- as.integer(base)
    end    <- as.integer(paste0(prefix, suffix))
    if (!is.na(start) && !is.na(end) && end >= start) {
      return(as.character(seq(start, end)))
    }
  }
  return(token)
}

bea_long <- bea |>
  mutate(
    # naics_multi_io: NAICS code appears in more than one BEA I-O industry;
    # trade flows through this code are inherently ambiguous at the BEA level.
    naics_multi_io = str_detect(naics_raw, "\\*"),
    naics_clean    = str_remove_all(naics_raw, "\\*")
  ) |>
  mutate(naics_tokens = str_split(naics_clean, ",\\s*")) |>
  unnest(naics_tokens) |>
  mutate(naics_tokens = str_trim(naics_tokens)) |>
  mutate(naics_expanded = map(naics_tokens, expand_bea_range)) |>
  unnest(naics_expanded) |>
  mutate(
    # Zero-pad purely numeric codes shorter than 6 digits (right-pad with 0).
    # Alphanumeric codes are left as-is.
    naics_6d = case_when(
      str_detect(naics_expanded, "^[0-9]+$") & nchar(naics_expanded) < 6 ~
        str_pad(naics_expanded, width = 6, side = "right", pad = "0"),
      TRUE ~ naics_expanded
    )
  ) |>
  select(bea_detail, naics_6d, naics_multi_io) |>
  distinct()


# ── 2. Load USITC Import Concordance ─────────────────────────────────────────

# col_types = "text" prevents readxl from type-guessing columns that are empty
# for the first ~125k rows and then switch to NAICS codes, which would cause
# those values to be coerced to NA.
usitc <- read_excel(
  "commodity_translation_wizard.xlsx",
  sheet     = "Import Concordance",
  col_types = "text"
) |>
  select(hts10, naics) |>
  mutate(
    # naics_ambiguous: USITC X-suffix notation (e.g. "11211X") indicates the
    # mapping is to a partial NAICS code; trailing X's mark indeterminate digits.
    naics_ambiguous = str_detect(naics, "X"),
    hts10 = str_pad(str_remove_all(hts10, "[^0-9]"), width = 10,
                    side = "left", pad = "0"),
    naics = case_when(
      str_detect(naics, "^[0-9]+$") ~
        str_pad(naics, width = 6, side = "left", pad = "0"),
      TRUE ~ naics
    )
  ) |>
  filter(!is.na(hts10), !is.na(naics)) |>
  distinct()


# ── 3. Build concordance ──────────────────────────────────────────────────────

# 3a. Exact 6-digit match (highest confidence)
concordance_exact <- bea_long |>
  inner_join(
    usitc |> filter(!naics_ambiguous),
    by = c("naics_6d" = "naics"),
    relationship = "many-to-many"
  ) |>
  mutate(match_type = "exact_6d")

# 3b. Ambiguous USITC NAICS (X-suffix), resolved via prefix match.
#     Verified resolutions (single BEA industry per prefix):
#       11211X → 1121A0  |  1123XX → 112300
#       31181X → 311810  |  33631X → 336310
#       31131X → 311300  |  31135X → 311300  (4-digit prefix)
#     Dropped: 31511X — no BEA match at any prefix depth (22 rows).
concordance_5d_ambiguous <- usitc |>
  filter(naics_ambiguous, !str_starts(naics, "31511")) |>
  mutate(naics_prefix = str_remove(naics, "X+$")) |>
  inner_join(
    bea_long |>
      mutate(naics_prefix = case_when(
        str_starts(naics_6d, "3113") ~ "3113",
        TRUE ~ str_sub(naics_6d, 1, 5)
      )),
    by = "naics_prefix",
    relationship = "many-to-many"
  ) |>
  mutate(match_type = "ambiguous_prefix") |>
  select(bea_detail, naics_6d, hts10, match_type, naics_multi_io)

# 3c. 5-digit prefix fallback for BEA codes unmatched at exact 6-digit
unmatched_bea <- bea_long |>
  anti_join(concordance_exact, by = c("bea_detail", "naics_6d"))

concordance_5d <- unmatched_bea |>
  mutate(naics_5d = str_sub(naics_6d, 1, 5)) |>
  inner_join(
    usitc |>
      filter(!naics_ambiguous) |>
      mutate(naics_5d = str_sub(naics, 1, 5)),
    by = "naics_5d",
    relationship = "many-to-many"
  ) |>
  mutate(match_type = "fallback_5d") |>
  select(bea_detail, naics_6d, hts10, match_type, naics_multi_io)

# 3d. Bind and deduplicate: where the same bea_detail/hts10 pair appears via
#     multiple match paths, retain only the highest-confidence match.
concordance <- bind_rows(
  concordance_exact |> select(bea_detail, naics_6d, hts10, match_type, naics_multi_io),
  concordance_5d_ambiguous,
  concordance_5d
) |>
  mutate(match_rank = case_when(
    match_type == "exact_6d"         ~ 1L,
    match_type == "ambiguous_prefix" ~ 2L,
    match_type == "fallback_5d"      ~ 3L
  )) |>
  group_by(bea_detail, hts10) |>
  slice_min(match_rank, with_ties = FALSE) |>
  ungroup() |>
  select(-match_rank)


# ── 4. Coverage diagnostics ───────────────────────────────────────────────────

n_bea_total   <- n_distinct(bea_long$bea_detail)
n_bea_matched <- n_distinct(concordance$bea_detail)
cat("BEA detail coverage:", n_bea_matched, "/", n_bea_total,
    sprintf("(%.1f%%)\n", 100 * n_bea_matched / n_bea_total))

cat("Dropped (31511X, no BEA match):",
    usitc |> filter(str_starts(naics, "31511")) |> nrow(), "rows\n")

cat("HTS-10 codes mapping to >1 BEA industry:",
    concordance |>
      group_by(hts10) |>
      summarise(n_bea = n_distinct(bea_detail)) |>
      filter(n_bea > 1) |>
      nrow(), "\n")

cat("\nMatch type breakdown:\n")
concordance |> count(match_type) |> print()

cat("\nHTS-to-BEA fan-out distribution:\n")
concordance |>
  group_by(hts10) |>
  summarise(n_bea = n_distinct(bea_detail)) |>
  count(n_bea) |>
  arrange(n_bea) |>
  print()


# ── 5. Save ───────────────────────────────────────────────────────────────────

saveRDS(concordance, "bea_naics_hts10_concordance.rds")
write.csv(concordance, "bea_naics_hts10_concordance.csv", row.names = FALSE)