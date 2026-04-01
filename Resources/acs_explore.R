# =============================================================================
# ACS Industry – NAICS – BEA Detail Concordance & Analysis File Prep
#
# Purpose:  Build a crosswalk from IPUMS ACS INDNAICS codes to BEA Detail
#           industry codes via 2017 NAICS, then merge onto the ACS person-level
#           file and export for Stata employment/occupation analysis.
#
# Inputs:   usa_00002.dta                              (IPUMS ACS 2023 1-year)
#           Margins_Before_Redefinitions_Detail.xlsx   (sheet: "NAICS Codes")
#
# Outputs:  acs_bea_concordance.rds / .csv   — crosswalk table
#           acs_analysis.dta                 — person-level file for Stata
#
# INDNAICS suffix conventions (IPUMS):
#   Numeric          — exact or truncated NAICS code; pad right to 6 digits
#   Z-suffix         — aggregates all NAICS codes sharing the numeric prefix
#   M / M[n]-suffix  — manually-constructed aggregation; prefix match
#   P / P[n]-suffix  — sub-industry split (e.g. 92811P1-P7 = military branches)
#   S-suffix         — sector-level aggregation; prefix match
#
# Match type hierarchy (highest to lowest confidence):
#   exact_6d > prefix_5d > prefix_4d > prefix_3d > prefix_2d
#
# Government codes (92xxx) are not in the BEA NAICS crosswalk and are
# manually assigned:
#   92811P1–P7, 9281P, 9211MP, 92MP, 92M1, 92M2, 923  → 928110 (Federal govt)
#   92113, 92119                                        → 928120 (State/local govt)
#   999920 (not employed 5+ years)                     → dropped
#   3MS, 4MS (sector-level aggregates)                 → dropped, too broad
#
# match_quality tiers (use as sample restriction in Stata):
#   "clean"      — exact_6d match, or manual government assignment
#   "acceptable" — prefix match, n_bea <= 4
#   "coarse"     — prefix match, 5 <= n_bea <= 15
#   "unusable"   — prefix match, n_bea > 15
#
# Coverage notes are printed to console during execution.
# =============================================================================

library(readxl)
library(haven)
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
    bea_detail = 4,
    naics_raw  = 7
  ) |>
  select(bea_detail, naics_raw) |>
  filter(
    !is.na(bea_detail),
    !is.na(naics_raw),
    str_trim(naics_raw) != "n.a."
  )

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
    naics_multi_io = str_detect(naics_raw, "\\*"),
    naics_clean    = str_remove_all(naics_raw, "\\*")
  ) |>
  mutate(naics_tokens = str_split(naics_clean, ",\\s*")) |>
  unnest(naics_tokens) |>
  mutate(naics_tokens = str_trim(naics_tokens)) |>
  mutate(naics_expanded = map(naics_tokens, expand_bea_range)) |>
  unnest(naics_expanded) |>
  mutate(
    naics_6d = case_when(
      str_detect(naics_expanded, "^[0-9]+$") & nchar(naics_expanded) < 6 ~
        str_pad(naics_expanded, width = 6, side = "right", pad = "0"),
      TRUE ~ naics_expanded
    )
  ) |>
  select(bea_detail, naics_6d, naics_multi_io) |>
  distinct()


# ── 2. Load and clean ACS INDNAICS ───────────────────────────────────────────

acs_raw <- read_dta("usa_00002.dta")

# Retain only employed persons (indnaics != 0) and variables needed downstream.
# OCCP and PERWT are the primary analysis variables; STATEFIP retained for
# geographic cuts. Replicate weights (REPWTP1-80) retained for Stata svyset.
acs <- acs_raw |>
  mutate(indnaics_str = str_trim(as.character(as_factor(indnaics)))) |>
  filter(indnaics_str != "0") |>
  mutate(
    # Classify suffix type for routing to correct matching logic
    indnaics_type = case_when(
      str_detect(indnaics_str, "^[0-9]+$")  ~ "numeric",
      str_detect(indnaics_str, "Z$")        ~ "Z_suffix",
      str_detect(indnaics_str, "M[0-9]*$")  ~ "M_suffix",
      str_detect(indnaics_str, "P[0-9]*$")  ~ "P_suffix",
      str_detect(indnaics_str, "S$")        ~ "S_suffix",
      TRUE                                  ~ "other"
    ),
    # Extract the numeric prefix for all codes — strip all trailing non-digits
    indnaics_prefix = str_remove(indnaics_str, "[A-Z][0-9]*$"),
    # Pad purely numeric codes to 6 digits (right-pad)
    indnaics_6d = case_when(
      indnaics_type == "numeric" & nchar(indnaics_str) < 6 ~
        str_pad(indnaics_str, width = 6, side = "right", pad = "0"),
      indnaics_type == "numeric" ~ indnaics_str,
      TRUE ~ NA_character_   # non-numeric handled via prefix matching below
    )
  )

# Warn if any "other" codes remain — should be 0 after pattern audit
n_other <- sum(acs$indnaics_type == "other")
if (n_other > 0) {
  warning(sprintf("%d rows have unclassified INDNAICS codes — inspect before proceeding.", n_other))
  acs |> filter(indnaics_type == "other") |> count(indnaics_str) |> print()
}


# ── 3. Build INDNAICS → BEA concordance ──────────────────────────────────────

# Get the distinct INDNAICS values to build the crosswalk, then join back
# to person-level data at the end.
indnaics_distinct <- acs |>
  distinct(indnaics_str, indnaics_type, indnaics_prefix, indnaics_6d)

# 3a. Exact 6-digit match for numeric codes
concordance_exact <- indnaics_distinct |>
  filter(indnaics_type == "numeric", !is.na(indnaics_6d)) |>
  inner_join(bea_long, by = c("indnaics_6d" = "naics_6d"),
             relationship = "many-to-many") |>
  mutate(match_type = "exact_6d") |>
  select(indnaics_str, bea_detail, naics_multi_io, match_type)

# 3b. Prefix matching for all non-numeric suffix types and any numeric codes
#     that failed exact match. Try progressively shorter prefixes (5→4→3→2).
#     For each prefix length, only attempt codes not yet matched at a higher
#     confidence level.

prefix_join <- function(indnaics_df, bea_df, prefix_len, already_matched) {
  candidates <- indnaics_df |>
    anti_join(already_matched, by = "indnaics_str") |>
    mutate(join_prefix = str_sub(indnaics_prefix, 1, prefix_len)) |>
    filter(nchar(join_prefix) == prefix_len)   # drop codes shorter than prefix_len
  
  if (nrow(candidates) == 0) return(tibble())
  
  bea_df |>
    mutate(join_prefix = str_sub(naics_6d, 1, prefix_len)) |>
    inner_join(candidates, by = "join_prefix", relationship = "many-to-many") |>
    mutate(match_type = paste0("prefix_", prefix_len, "d")) |>
    select(indnaics_str, bea_detail, naics_multi_io, match_type)
}

# Pool of codes needing prefix matching:
# - all non-numeric suffix types
# - numeric codes unmatched at exact 6-digit
needs_prefix <- indnaics_distinct |>
  filter(
    indnaics_type != "numeric" |
      !indnaics_str %in% concordance_exact$indnaics_str
  )

concordance_5d <- prefix_join(needs_prefix, bea_long, 5, concordance_exact)
concordance_4d <- prefix_join(needs_prefix, bea_long, 4,
                              bind_rows(concordance_exact, concordance_5d))
concordance_3d <- prefix_join(needs_prefix, bea_long, 3,
                              bind_rows(concordance_exact, concordance_5d, concordance_4d))
concordance_2d <- prefix_join(needs_prefix, bea_long, 2,
                              bind_rows(concordance_exact, concordance_5d,
                                        concordance_4d, concordance_3d))

# 3c. Manual government assignments
#     92xxx codes are absent from the BEA NAICS crosswalk because government
#     enterprises are handled separately in the BEA accounts. Assigned here
#     based on NAICS 928110 (Federal, including military) and 928120 (State/local).
#     999920 (not employed 5+ years) and sector-level aggregates 3MS/4MS are
#     dropped as unassignable.
govt_federal <- c("92811P1","92811P2","92811P3","92811P4",
                  "92811P5","92811P6","92811P7","9281P",
                  "9211MP","92MP","92M1","92M2","923")
govt_state   <- c("92113","92119")
drop_codes   <- c("999920","3MS","4MS")

concordance_govt <- tibble(
  indnaics_str   = c(govt_federal, govt_state),
  bea_detail     = c(rep("928110", length(govt_federal)),
                     rep("928120", length(govt_state))),
  naics_multi_io = FALSE,
  match_type     = "manual_govt"
) |>
  filter(indnaics_str %in% indnaics_distinct$indnaics_str)

# 3d. Bind and deduplicate — retain highest-confidence match per indnaics/BEA pair
concordance <- bind_rows(
  concordance_exact,
  concordance_5d,
  concordance_4d,
  concordance_3d,
  concordance_2d,
  concordance_govt
) |>
  filter(!indnaics_str %in% drop_codes) |>
  mutate(match_rank = case_when(
    match_type == "exact_6d"    ~ 1L,
    match_type == "manual_govt" ~ 1L,   # equivalent confidence to exact
    match_type == "prefix_5d"   ~ 2L,
    match_type == "prefix_4d"   ~ 3L,
    match_type == "prefix_3d"   ~ 4L,
    match_type == "prefix_2d"   ~ 5L
  )) |>
  group_by(indnaics_str, bea_detail) |>
  slice_min(match_rank, with_ties = FALSE) |>
  ungroup() |>
  select(-match_rank)


# ── 4. Coverage diagnostics ───────────────────────────────────────────────────

n_indnaics_total   <- n_distinct(indnaics_distinct$indnaics_str)
n_indnaics_matched <- n_distinct(concordance$indnaics_str)
cat("INDNAICS coverage:", n_indnaics_matched, "/", n_indnaics_total,
    sprintf("(%.1f%%)\n", 100 * n_indnaics_matched / n_indnaics_total))

cat("\nMatch type breakdown:\n")
concordance |>
  distinct(indnaics_str, match_type) |>
  count(match_type) |>
  arrange(match_type) |>
  print()

# Fan-out: how many BEA industries does each INDNAICS code map to?
fanout <- concordance |>
  group_by(indnaics_str) |>
  summarise(n_bea = n_distinct(bea_detail), .groups = "drop")

# Attach match_quality tier to concordance
# Use as sample restriction variable in Stata:
#   "clean"      — exact match or manual govt assignment; use for robustness check
#   "acceptable" — prefix match, n_bea <= 4; fractional weighting reliable
#   "coarse"     — prefix match, 5–15 BEA industries; interpret with caution
#   "unusable"   — n_bea > 15; fractional weighting not meaningful, exclude
concordance <- concordance |>
  left_join(fanout, by = "indnaics_str") |>
  mutate(match_quality = case_when(
    match_type %in% c("exact_6d", "manual_govt") ~ "clean",
    n_bea <= 4                                   ~ "acceptable",
    n_bea <= 15                                  ~ "coarse",
    TRUE                                         ~ "unusable"
  ))

cat("\nINDNAICS-to-BEA fan-out distribution:\n")
fanout |> count(n_bea) |> arrange(n_bea) |> print()

cat("\nMatch quality breakdown:\n")
concordance |>
  distinct(indnaics_str, match_quality) |>
  count(match_quality) |>
  arrange(match_quality) |>
  print()

cat("\nDropped codes (unassignable):", paste(drop_codes, collapse = ", "), "\n")

# Flag high fan-out cases for manual review
high_fanout <- fanout |>
  filter(n_bea > 2) |>
  left_join(concordance |> group_by(indnaics_str) |>
              summarise(bea_list = paste(sort(unique(bea_detail)), collapse = ", "),
                        .groups = "drop"),
            by = "indnaics_str") |>
  arrange(desc(n_bea))

if (nrow(high_fanout) > 0) {
  cat("\nHigh fan-out INDNAICS codes (>2 BEA industries) — review recommended:\n")
  print(high_fanout, n = Inf)
}

# Unmatched INDNAICS codes
unmatched <- indnaics_distinct |>
  anti_join(concordance, by = "indnaics_str") |>
  select(indnaics_str, indnaics_type)

if (nrow(unmatched) > 0) {
  cat("\nUnmatched INDNAICS codes:\n")
  print(unmatched, n = Inf)
}


# ── 5. Merge concordance onto ACS person-level file ──────────────────────────

# Where a person's INDNAICS maps to multiple BEA industries (fan-out > 1),
# this join will produce multiple rows per person. The Stata analysis must
# account for this — either by restricting to fan-out == 1 cases, or by
# using fractional weights (perwt / n_bea) for multi-matched persons.
# The n_bea column enables either approach.

acs_merged <- acs |>
  filter(!indnaics_str %in% drop_codes) |>
  left_join(fanout, by = "indnaics_str") |>
  left_join(
    concordance |> select(-n_bea),
    by = "indnaics_str",
    relationship = "many-to-many"
  ) |>
  mutate(perwt_adj = perwt / n_bea)

# ── 6. Export ─────────────────────────────────────────────────────────────────

# Concordance table
saveRDS(concordance, "acs_bea_concordance.rds")
write.csv(concordance, "acs_bea_concordance.csv", row.names = FALSE)

# Person-level analysis file for Stata
# Retains: person identifiers, weights, BEA industry assignment, OCCP, STATEFIP
# Drop the intermediate string columns to keep the .dta clean
acs_merged |>
  select(
    -indnaics_str, -indnaics_type, -indnaics_prefix, -indnaics_6d
  ) |>
  write_dta("acs_analysis.dta")

cat("\nDone. Files written:\n")
cat("  acs_bea_concordance.rds / .csv\n")
cat("  acs_analysis.dta\n")