These two dbt models are examples based on the shape of campaign reporting and measurement work. No real client-data was used.

## Models

1. `int_marketing_event_conversion_path.sql`
   - Granularity: one row per deduped conversion event
   - Shows: event cleaning, conversion dedupe, conversion journey rollup, time-to-conversion buckets, campaign enrichment

2. `rpt_campaign_delivery_quality_daily.sql`
   - Granularity: one row per campaign per activity date
   - Shows: campaign delivery aggregation, goal/budget logic, conversion-path reconciliation, reporting health flags

## Assumed upstream models

These names are placeholders and can be adjusted to match any real dbt project:

- `stg_marketing_events`
- `dim_campaigns`
- `fct_campaign_daily_delivery`