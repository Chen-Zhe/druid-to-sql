## Convert Druid queries to SQL queries

A simple Python script to convert Druid queries to SQL queries.

Currently, it doesn't support `postAggregations`, `granularity` and more complex filter/aggregation types (such as `quantilesDoublesSketch`). Due to type unawareness, it can't differentiate between selector on a multi-value dimension, and a single-value dimension.