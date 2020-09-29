

# Module reliable #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Reliable is an OTP application that offers a solution to the problem of
ensuring a sequence of Riak KV operations are guaranteed to occur, and to
occur in order.

<a name="description"></a>

## Description ##
The problem arises when one wants to write multiple associated objects to
Riak KV which does not support multi-key atomicity, including but not
exclusively, the update of application managed secondary indices after a
write.
<a name="types"></a>

## Data Types ##




### <a name="type-opts">opts()</a> ###


<pre><code>
opts() = #{partition_key =&gt; binary()}
</code></pre>




### <a name="type-scheduled_item">scheduled_item()</a> ###


<pre><code>
scheduled_item() = <a href="reliable_worker.md#type-work_item">reliable_worker:work_item()</a> | fun(() -&gt; <a href="reliable_worker.md#type-work_item">reliable_worker:work_item()</a>)
</code></pre>




### <a name="type-work_id">work_id()</a> ###


<pre><code>
work_id() = <a href="reliable_worker.md#type-work_id">reliable_worker:work_id()</a>
</code></pre>




### <a name="type-work_item">work_item()</a> ###


<pre><code>
work_item() = [{<a href="reliable_worker.md#type-work_item_id">reliable_worker:work_item_id()</a>, <a href="reliable_worker.md#type-work_item">reliable_worker:work_item()</a>}]
</code></pre>




### <a name="type-workflow_item">workflow_item()</a> ###


<pre><code>
workflow_item() = {Id::<a href="#type-workflow_item_id">workflow_item_id()</a>, {update | delete, <a href="#type-scheduled_item">scheduled_item()</a>}}
</code></pre>




### <a name="type-workflow_item_id">workflow_item_id()</a> ###


<pre><code>
workflow_item_id() = term()
</code></pre>




### <a name="type-workflow_opts">workflow_opts()</a> ###


<pre><code>
workflow_opts() = #{partition_key =&gt; binary(), on_terminate =&gt; fun((Reason::any()) -&gt; any())}
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#abort-1">abort/1</a></td><td>When called within the functional object in <a href="#workflow-2"><code>workflow/2</code></a>,
makes the workflow silently return the tuple {aborted, Reason} as the
error reason.</td></tr><tr><td valign="top"><a href="#add_workflow_items-1">add_workflow_items/1</a></td><td>Adds a workflow item to the workflow stack.</td></tr><tr><td valign="top"><a href="#add_workflow_precedence-2">add_workflow_precedence/2</a></td><td>Relates on or more workflow items in a precedence relationship.</td></tr><tr><td valign="top"><a href="#enqueue-2">enqueue/2</a></td><td></td></tr><tr><td valign="top"><a href="#enqueue-3">enqueue/3</a></td><td></td></tr><tr><td valign="top"><a href="#ensure_in_workflow-0">ensure_in_workflow/0</a></td><td>Fails with a <code>no_workflow</code> exception if the calling process doe not
have a workflow initiated.</td></tr><tr><td valign="top"><a href="#find_workflow_item-1">find_workflow_item/1</a></td><td>
Fails with a <code>no_workflow</code> exception if the calling process doe not
have a workflow initiated.</td></tr><tr><td valign="top"><a href="#get_workflow_item-1">get_workflow_item/1</a></td><td>Returns a workflow item that was previously added to the workflow stack
with the <a href="#add_workflow_items-2"><code>add_workflow_items/2</code></a> function.</td></tr><tr><td valign="top"><a href="#is_in_workflow-0">is_in_workflow/0</a></td><td>Returns true if the process has a workflow context.</td></tr><tr><td valign="top"><a href="#is_nested_workflow-0">is_nested_workflow/0</a></td><td>Returns true if the current workflow is nested i.e.</td></tr><tr><td valign="top"><a href="#workflow-1">workflow/1</a></td><td>Equivalent to calling <a href="#workflow-2"><code>workflow/2</code></a> with and empty map passed as
the <code>Opts</code> argument.</td></tr><tr><td valign="top"><a href="#workflow-2">workflow/2</a></td><td>Executes the functional object <code>Fun</code> as a Reliable workflow, i.e.</td></tr><tr><td valign="top"><a href="#workflow_id-0">workflow_id/0</a></td><td>Returns the workflow identifier or undefined if there is no workflow
initiated for the calling process.</td></tr><tr><td valign="top"><a href="#workflow_nesting_level-0">workflow_nesting_level/0</a></td><td>Returns the current worflow nesting level.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="abort-1"></a>

### abort/1 ###

<pre><code>
abort(Reason::any()) -&gt; no_return()
</code></pre>
<br />

When called within the functional object in [`workflow/2`](#workflow-2),
makes the workflow silently return the tuple {aborted, Reason} as the
error reason.

Termination of a Babel workflow means that an exception is thrown to an
enclosing catch. Thus, the expression `catch babel:abort(foo)` does not
terminate the workflow.

<a name="add_workflow_items-1"></a>

### add_workflow_items/1 ###

<pre><code>
add_workflow_items(L::[<a href="#type-workflow_item">workflow_item()</a>]) -&gt; ok | no_return()
</code></pre>
<br />

Adds a workflow item to the workflow stack.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="add_workflow_precedence-2"></a>

### add_workflow_precedence/2 ###

<pre><code>
add_workflow_precedence(As::<a href="#type-workflow_item_id">workflow_item_id()</a> | [<a href="#type-workflow_item_id">workflow_item_id()</a>], Bs::<a href="#type-workflow_item_id">workflow_item_id()</a> | [<a href="#type-workflow_item_id">workflow_item_id()</a>]) -&gt; ok | no_return()
</code></pre>
<br />

Relates on or more workflow items in a precedence relationship. This
relationship is used by the [`workflow/2`](#workflow-2) function to determine the
workflow execution order based on the resulting precedence graph topsort
calculation.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="enqueue-2"></a>

### enqueue/2 ###

<pre><code>
enqueue(WorkId::<a href="#type-work_id">work_id()</a>, WorkItems::[<a href="#type-work_item">work_item()</a>]) -&gt; ok
</code></pre>
<br />

<a name="enqueue-3"></a>

### enqueue/3 ###

<pre><code>
enqueue(WorkId::<a href="#type-work_id">work_id()</a>, WorkItems::[<a href="#type-work_item">work_item()</a>], Opts::<a href="#type-opts">opts()</a>) -&gt; ok
</code></pre>
<br />

<a name="ensure_in_workflow-0"></a>

### ensure_in_workflow/0 ###

<pre><code>
ensure_in_workflow() -&gt; ok | no_return()
</code></pre>
<br />

Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="find_workflow_item-1"></a>

### find_workflow_item/1 ###

<pre><code>
find_workflow_item(Id::<a href="#type-workflow_item_id">workflow_item_id()</a>) -&gt; {ok, <a href="#type-workflow_item">workflow_item()</a>} | error | no_return()
</code></pre>
<br />

Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="get_workflow_item-1"></a>

### get_workflow_item/1 ###

<pre><code>
get_workflow_item(Id::<a href="#type-workflow_item_id">workflow_item_id()</a>) -&gt; <a href="#type-workflow_item">workflow_item()</a> | no_return() | no_return()
</code></pre>
<br />

Returns a workflow item that was previously added to the workflow stack
with the [`add_workflow_items/2`](#add_workflow_items-2) function.
Fails with a `badkey` exception if there is no workflow item identified by
`Id`.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="is_in_workflow-0"></a>

### is_in_workflow/0 ###

<pre><code>
is_in_workflow() -&gt; boolean()
</code></pre>
<br />

Returns true if the process has a workflow context.
See [`workflow/2`](#workflow-2).

<a name="is_nested_workflow-0"></a>

### is_nested_workflow/0 ###

<pre><code>
is_nested_workflow() -&gt; boolean() | no_return()
</code></pre>
<br />

Returns true if the current workflow is nested i.e. has a parent
workflow.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="workflow-1"></a>

### workflow/1 ###

<pre><code>
workflow(Fun::fun(() -&gt; any())) -&gt; {ok, {WorkId::binary(), ResultOfFun::any()}} | {error, Reason::any()} | no_return()
</code></pre>
<br />

Equivalent to calling [`workflow/2`](#workflow-2) with and empty map passed as
the `Opts` argument.

<a name="workflow-2"></a>

### workflow/2 ###

<pre><code>
workflow(Fun::fun(() -&gt; any()), Opts::<a href="#type-opts">opts()</a>) -&gt; {ok, {WorkId::binary(), ResultOfFun::any()}} | {error, Reason::any()} | no_return()
</code></pre>
<br />

Executes the functional object `Fun` as a Reliable workflow, i.e.
ordering and scheduling all resulting Riak KV object writes and deletes.

Any function that executes inside the workflow that wants to be able to
schedule work to Riak KV, needs to use the infrastructure provided in this
module to add workflow items to the workflow stack
(see [`add_workflow_items/1`](#add_workflow_items-1)) and to add the precedence amongst them
(see [`add_workflow_precedence/2`](#add_workflow_precedence-2)).

Any other operation, including reading and writing from/to Riak KV by
directly using the Riak Client library will work as normal and
will not affect the workflow. Only by calling the special functions in this
module you can add work items to the workflow.

If something goes wrong inside the workflow as a result of a user
error or general exception, the entire workflow is terminated and the
function raises an exception. In case of an internal error, the function
returns the tuple `{error, Reason}`.

If everything goes well, the function returns the tuple
`{ok, {WorkId, ResultOfFun}}` where `WorkId` is the identifier for the
workflow scheduled by Reliable and `ResultOfFun` is the value of the last
expression in `Fun`.

> Notice that calling this function schedules the work to Reliable, you need
to use the WorkId to check with Reliable the status of the workflow
execution.

The resulting workflow execution will schedule the writes and deletes in the
order defined by the dependency graph constructed using
[`add_workflow_precedence/2`](#add_workflow_precedence-2).

> If you want to manually determine the execution order of the workflow you
should use the [`enqueue/2`](#enqueue-2) function instead.

The `Opts` argument offers the following options:

* `on_terminate` – a functional object `fun((Reason :: any()) -> ok)`. This
function will be evaluated before the call terminates. In case of successful
termination the value `normal` will be  passed as argument. Otherwise, in
case of error, the error reason will be passed as argument. This allows you
to perform a cleanup after the workflow execution e.g. returning a Riak
connection object to a pool. Notice that this function might be called
multiple times in the case of nested workflows. If you need to conditionally
perform a cleanup operation within the functional object only at the end of
the workflow call, you can use the function `is_nested_workflow/0`
to take a decision.

Calls to this function can be nested and the result is exactly the same as it
would without a nested call i.e. nesting workflows does not provide any kind
of stratification and thus there is no implicit precedence relationship
between workflow items scheduled at different nesting levels, unless you
explecitly create those relationships by using the
[`add_workflow_precedence/2`](#add_workflow_precedence-2) function.

<a name="workflow_id-0"></a>

### workflow_id/0 ###

<pre><code>
workflow_id() -&gt; binary() | no_return()
</code></pre>
<br />

Returns the workflow identifier or undefined if there is no workflow
initiated for the calling process.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

<a name="workflow_nesting_level-0"></a>

### workflow_nesting_level/0 ###

<pre><code>
workflow_nesting_level() -&gt; pos_integer() | no_return()
</code></pre>
<br />

Returns the current worflow nesting level.
Fails with a `no_workflow` exception if the calling process doe not
have a workflow initiated.

