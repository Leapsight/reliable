

# Module reliable_config #
* [Function Index](#index)
* [Function Details](#functions)

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#instance-1">instance/1</a></td><td></td></tr><tr><td valign="top"><a href="#instance_name-0">instance_name/0</a></td><td></td></tr><tr><td valign="top"><a href="#instances-0">instances/0</a></td><td></td></tr><tr><td valign="top"><a href="#number_of_partitions-0">number_of_partitions/0</a></td><td></td></tr><tr><td valign="top"><a href="#riak_host-0">riak_host/0</a></td><td></td></tr><tr><td valign="top"><a href="#riak_port-0">riak_port/0</a></td><td></td></tr><tr><td valign="top"><a href="#setup-0">setup/0</a></td><td></td></tr><tr><td valign="top"><a href="#storage_backend-0">storage_backend/0</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="instance-1"></a>

### instance/1 ###

<pre><code>
partition(Key::binary() | undefined) -&gt; binary()
</code></pre>
<br />

<a name="instance_name-0"></a>

### instance_name/0 ###

<pre><code>
instance_name() -&gt; [binary()]
</code></pre>
<br />

<a name="instances-0"></a>

### instances/0 ###

<pre><code>
partitions() -&gt; [binary()]
</code></pre>
<br />

<a name="number_of_partitions-0"></a>

### number_of_partitions/0 ###

<pre><code>
number_of_partitions() -&gt; [binary()]
</code></pre>
<br />

<a name="riak_host-0"></a>

### riak_host/0 ###

<pre><code>
riak_host() -&gt; list()
</code></pre>
<br />

<a name="riak_port-0"></a>

### riak_port/0 ###

<pre><code>
riak_port() -&gt; integer()
</code></pre>
<br />

<a name="setup-0"></a>

### setup/0 ###

<pre><code>
setup() -&gt; ok
</code></pre>
<br />

<a name="storage_backend-0"></a>

### storage_backend/0 ###

<pre><code>
storage_backend() -&gt; module()
</code></pre>
<br />

