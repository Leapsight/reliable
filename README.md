reliable
=====

An OTP application

Build
-----

    $ rebar3 compile

Bucket Types
-----

riak-admin bucket-type create sets '{"props":{"datatype":"set"}}'
riak-admin bucket-type status sets
riak-admin bucket-type activate sets
