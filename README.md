# Getting started

<!-- ![](./Flow.png) -->

## To deploy a development copy of this project

```
databricks bundle deploy --target dev
```

## To run the Bronze onetime backfill pipeline

```
databricks bundle run -t dev bronze_citi_bike_onetime_backfill_pipeline
```

## To run the Bronze continuous pipeline

```
databricks bundle run -t dev bronze_citi_bike_continuous_pipeline
```

## To run the Silver pipeline

```
databricks bundle run -t dev silver_citi_bike_pipeline
```

## To run the Gold pipeline

```
databricks bundle run -t dev gold_citi_bike_pipeline
```


databricks bundle run -t dev silver_citi_bike_pipeline --full-refresh silver_citi_trip_data

--full-refresh strings   List of tables to reset and recompute.
--full-refresh-all       Perform a full graph reset and recompute.
--refresh strings        List of tables to update.
--refresh-all            Perform a full graph update.
--validate-only          Perform an update to validate graph correctness.