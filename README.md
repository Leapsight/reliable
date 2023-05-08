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
