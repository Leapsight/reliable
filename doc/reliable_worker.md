

# Module reliable_worker #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-work">work()</a> ###


<pre><code>
work() = {WorkId::<a href="#type-work_id">work_id()</a>, Payload::[{<a href="#type-work_item_id">work_item_id()</a>, <a href="#type-work_item">work_item()</a>, <a href="#type-work_item_result">work_item_result()</a>}]}
</code></pre>




### <a name="type-work_id">work_id()</a> ###


<pre><code>
work_id() = term()
</code></pre>




### <a name="type-work_item">work_item()</a> ###


<pre><code>
work_item() = {node(), module(), function(), [term()]}
</code></pre>




### <a name="type-work_item_id">work_item_id()</a> ###


<pre><code>
work_item_id() = integer()
</code></pre>




### <a name="type-work_item_result">work_item_result()</a> ###


<pre><code>
work_item_result() = term()
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#enqueue-2">enqueue/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_continue-2">handle_continue/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="code_change-3"></a>

### code_change/3 ###

`code_change(OldVsn, State, Extra) -> any()`

<a name="enqueue-2"></a>

### enqueue/2 ###

<pre><code>
enqueue(Work::<a href="#type-work">work()</a>, PartitionKey::binary() | undefined) -&gt; ok | {error, term()}
</code></pre>
<br />

<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(Request, From, State) -> any()`

<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(Msg, State) -> any()`

<a name="handle_continue-2"></a>

### handle_continue/2 ###

`handle_continue(X1, State) -> any()`

<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(Info, State) -> any()`

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`

<a name="start_link-2"></a>

### start_link/2 ###

`start_link(Name, Bucket) -> any()`

<a name="terminate-2"></a>

### terminate/2 ###

`terminate(Reason, State) -> any()`

