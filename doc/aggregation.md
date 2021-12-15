# Specifying what to aggregate

Each metric type has different list of aggregates which can be counted:

| Metric type name | statsd name | Aggregates |
| ---           | ---   | --- |
| counter       | c     | value, updates, rate |
| timer         | ms    | last, min, max, sum, median, mean, updates, rate, percentile-\* |
| gauge         | g     | value, updates, rate |
| set           | s     | count, updates, rate |

`value` is the aggregate of the resulting value of that type.

`percentile-<num>` can be used to add *any* percentile by adding a positive integer number to it.
It will then be threated in the same way as appending a "0." before that number. For example, "percentile-9999" will become 0.9999th percentile.
By default the percentiles for timer are: 75th, 95th, 98th, 99th and 999th.

`updates` is a special aggregate, showing the number of metrics with this name that came in during the aggregation period.
It is counted always, even if disabled by configuration and can be additionally filtered by `aggregation.update-count-threshold` parameter.

`rate` aggregate takes sampling rate in account, and tries to restore the original value rate on the metric source rather that "system" rate of receiving metric on the server

By default, bioyino counts all the aggregates. This behaviour can be changed by `aggregation.aggregates` option.
If a type is not specified, the default value(meaning all aggregates) will be used.

Example:
```
[ aggregation ]
aggregates.gauge = { "value" }
aggretates.set = {}
```

In the example above:
* the `timer` type will have all the default aggregates because aggregates.ms is not specified
* the `gauge` type will have only `value` aggregate
* the `set` type will have no aggregates at all and will not appear

# Metric naming during aggregation

Bioyino has numerous ways to define naming options for the incoming metrics.

## Intro

There are some approaches to naming, evolved for time. The people with different aged infrastructures usually want names to look in different ways.

For the sake of example imagine having following metrics:

* `some.ms-typed.one` with type timer aggregated with `sum` and `max` aggregates
* `some.gauge-typed.one` with type `gauge` aggregated using `value` and `updates` aggregates
* `some.set-typed.one` with type `set` aggregated using `count` aggregate

Some older approaches consider using prefixes and postfixes to distribute metrics into namespaces by type or aggregate name.

So in our example metrics must be aggregated like this:
* `some.ms-typed.one` must become `global.namespace.some.ms-typed.one.sum` and `global.namespace.some.ms-typed.one.upper`
* `some.gauge-typed.one` must become `global.namespace.gauges.some.gauge-typed.one` and `global.namespace.debug-updates.gauges.some.gauge-typed.one`
* `some.set-typed.one` must become `global.namespace.some.set-typed.one;agg=count;value=1`

Note the important differences in aggregation here:
* someone wants the compatibility with other statsd and name `max` aggregate as `upper`
* someone wants the compatibility with the original statsd implementation and put all gauge metric values into another namespace
* someone wants the separate namespace for update counting on gauges since this is an internal aggregate that needs to be separated from business aggregates
* someone wants the modern way aggregation with tags and (e.g. for better prometheus support) leaving the name as is and putting everything into tags

Bioyino tries to support almost all of such cases.

## Defining naming for aggregates

There is the `naming` map, split by metric type containing all the options. Since there is no top-level setting in this map, it is better to use it with the keys.

The important thing here is that it includes the `default` key.

The solution of all the wishes above is like this

```
[aggregation]
# specify only required aggregates for timers and sets
aggregates.timer = ["sum", "max"]
aggregates.set = [ "count" ]

[naming.default]
prefix="global.namespace"

# set compatibility mode for max aggregate
postfixes = { "max" = "upper" }

[naming.gauge]
# override namespace requirements for gauges
prefix = "global.namespace.gauges"
prefix-overrides = {"updates" = "global.namespace.debug-updates.gauges" }

[naming.set]
# Change the destination of aggregation for sets
destination = "tag"
tag = "agg"
```
