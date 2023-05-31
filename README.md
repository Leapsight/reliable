# reliable

An OTP application

## Build

    $ rebar3 compile

## Bucket Types

riak-admin bucket-type create sets '{"props":{"datatype":"set"}}'
riak-admin bucket-type status sets
riak-admin bucket-type activate sets

## Configuration

### Via `sys.config`

```erlang
[
    {reliable, [
        %% Use Riak iteslef as backend for work queues
        {backend, reliable_riak_store_backend},
        %%
        {riak_host, "127.0.0.1"},
        {riak_port, 8087},
        {riak_pool, #{
            min_size => 3,
            max_size => 10
        }},
        %% At the moment we do not support clustering, so if you have more
        %% than one app instance, give each on a distinct identifier in binary
        %% format
        {instances, [
            <<"babel_test-0">>
        ]},
        {instance_name, <<"babel_test-0">>},
        %% Each instance has a number of partitions N, this maps to the number
        %% of work queues and worker. Work is assigned to partitions using the
        %% option `partition_key'
        {number_of_partitions, 3},
        {worker_retry, [
            {backoff_type, jitter},
            {backoff_min, timer:seconds(1)},
            {backoff_max, timer:minutes(1)},
            {deadline, timer:minutes(15)},
            {max_retries, 50},
        ]}

    ]}
]
```


## Telemetry Events

`reliable` uses the `telemetry` library for instrumentation.

A Telemetry event is made up of the following:

* `name` - A list of atoms that uniquely identifies the event.

* `measurements` - A map of atom keys (e.g. duration) and numeric values.

* `metadata` - A map of key-value pairs that can be used for tagging metrics.

All events time measurements represent time as native units, so to convert the from native units to another unit e.g. `millisecond` you can use:

```erlang
erlang:convert_time_unit(Value, native, millisecond)
```

### [reliable, enqueue, start]

##### Measurements
```erlang
#{
    monotonic_time => -576460737043530333,
    system_time => 1685448802402222902
}
```

##### Metadata
```erlang
#{
    partition => <<"babel_test-0_partition_1">>,
    payload => #{items => [a,b]},
    telemetry_span_context => #Ref<0.1339438288.4109369345.111931>,
    work_id => <<"00008LrTASfNp6sFVEPlUcYkIEV">>,
    work_ref => {reliable_work_ref,'babel_test-0_partition_1',<<"00008LrTASfNp6sFVEPlUcYkIEV">>}
}
```


### [reliable, enqueue, stop]

##### Measurements
```erlang
#{
    retries => 0,
    duration => 3944759,
    monotonic_time => -576460745888161345
}
```

##### Metadata
```erlang
#{
    partition => <<"babel_test-0_partition_1">>,
    payload => #{items => [a,b]},
    telemetry_span_context => #Ref<0.1339438288.4109369345.111931>,
    work_id => <<"00008LrTASfNp6sFVEPlUcYkIEV">>,
    work_ref => {reliable_work_ref,'babel_test-0_partition_1',<<"00008LrTASfNp6sFVEPlUcYkIEV">>}
}
```

### [reliable, enqueue, exception]

##### Measurements
```erlang
#{
    duration => 3944759,
    monotonic_time => -576460745888161345
}
```

##### Metadata
```erlang
#{
    class => error,
    reason => foo,
    stacktrace => ...,
    partition => <<"babel_test-0_partition_1">>,
    payload => #{items => [a,b]},
    telemetry_span_context => #Ref<0.1339438288.4109369345.111931>,
    work_id => <<"00008LrTASfNp6sFVEPlUcYkIEV">>,
    work_ref => {reliable_work_ref,'babel_test-0_partition_1',<<"00008LrTASfNp6sFVEPlUcYkIEV">>}
}
```


### [reliable, work, execute, start]
TBD
### [reliable, work, execute, stop]
TBD
### [reliable, work, execute, exception]
TBD

### [reliable, task, execute, start]
TBD
### [reliable, task, execute, stop]
TBD
### [reliable, task, execute, exception]
TBD



