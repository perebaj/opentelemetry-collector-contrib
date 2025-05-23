<!-- status autogenerated section -->
| Status        |           |
| ------------- |-----------|
| Stability     | [development]: metrics   |
| Distributions | [] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Aprocessor%2Fmetricstarttime%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Aprocessor%2Fmetricstarttime) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Aprocessor%2Fmetricstarttime%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Aprocessor%2Fmetricstarttime) |
| Code coverage | [![codecov](https://codecov.io/github/open-telemetry/opentelemetry-collector-contrib/graph/main/badge.svg?component=processor_metricstarttime)](https://app.codecov.io/gh/open-telemetry/opentelemetry-collector-contrib/tree/main/?components%5B0%5D=processor_metricstarttime&displayType=list) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@dashpole](https://www.github.com/dashpole), [@ridwanmsharif](https://www.github.com/ridwanmsharif) |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development
<!-- end autogenerated section -->

## Description

The metric start time processor (`metricstarttime`) is used to set the start
time for metric points with a cumulative aggregation temporality. It is
commonly used with the `prometheus` receiver, which usually produces metric
points without a start time.

## Configuration

Configuration allows configuring the strategy used to set the start time.

```yaml
processors:
    # processor name: metricstarttime
    metricstarttime:

        # specify the strategy to use for setting the start time
        strategy: true_reset_point
```

### Strategy: True Reset Point

The `true_reset_point` strategy handles missing start times for cumulative
points by producing a stream of points that starts with a
[True Reset Point](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#cumulative-streams-inserting-true-reset-points).
The true reset point has its start time set to its end timestamp. It is meant
to indicate the absolute value of the cumulative point when the collector first
observed it. Subsequent points re-use the start timestamp of the initial True
Reset point.

Pros:

* The absolute value of the cumulative metric is preserved.
* It is possible to calculate the correct rate between any two points since the timestamps and values are not modified.

Cons:

* This strategy is **stateful** because the initial True Reset point is necessary to properly calculate rates on subsequent points.
* The True Reset point doesn't make sense semantically. It has a zero duration, but non-zero values.
* Many backends reject points with equal start and end timestamps.
    * If the True Reset point is rejected, the next point will appear to have a very large rate.

### Strategy: Subtract Initial Point

The `subtract_initial_point` strategy handles missing start times for
cumulative points by dropping the first point in a cumulative series,
"subtracting" that point's value from subsequent points and using the initial
point's timestamp as the start timestamp for subsequent points.

Pros:

* Cumulative semantics are preserved. This means that for a point with a given `[start, end]` interval, the cumulative value occurred in that interval.
* Rates over resulting timeseries are correct, even if points are lost. This strategy is not stateful.

Cons:

* The absolute value of counters is modified. This is generally not an issue, since counters are usually used to compute rates.
* The initial point is dropped, which loses information.
