<!--
GENERATED FILE. DO NOT EDIT.

Regenerate with: ./hack/gen-api-reference.sh
Source of truth: api/*.proto. Edit the proto comments, not this file.
-->

# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [common.proto](#common-proto)
    - [ApprovalRequest](#agentsessions-v1-ApprovalRequest)
    - [ApprovalResult](#agentsessions-v1-ApprovalResult)
    - [DataPart](#agentsessions-v1-DataPart)
    - [Error](#agentsessions-v1-Error)
    - [Event](#agentsessions-v1-Event)
    - [FilePart](#agentsessions-v1-FilePart)
    - [HarnessEnd](#agentsessions-v1-HarnessEnd)
    - [IdentityRef](#agentsessions-v1-IdentityRef)
    - [Lifecycle](#agentsessions-v1-Lifecycle)
    - [LogRecord](#agentsessions-v1-LogRecord)
    - [Message](#agentsessions-v1-Message)
    - [ModelCall](#agentsessions-v1-ModelCall)
    - [ModelCall.ParamsEntry](#agentsessions-v1-ModelCall-ParamsEntry)
    - [Origin](#agentsessions-v1-Origin)
    - [Origin.AttributesEntry](#agentsessions-v1-Origin-AttributesEntry)
    - [Part](#agentsessions-v1-Part)
    - [ReasoningPart](#agentsessions-v1-ReasoningPart)
    - [ResourceMetadata](#agentsessions-v1-ResourceMetadata)
    - [TextPart](#agentsessions-v1-TextPart)
    - [ToolCall](#agentsessions-v1-ToolCall)
    - [ToolResult](#agentsessions-v1-ToolResult)
    - [Usage](#agentsessions-v1-Usage)
  
    - [EventKind](#agentsessions-v1-EventKind)
    - [Lifecycle.Kind](#agentsessions-v1-Lifecycle-Kind)
    - [Mediation](#agentsessions-v1-Mediation)
  
- [harness.proto](#harness-proto)
    - [Cancel](#agentsessions-v1-Cancel)
    - [Capabilities](#agentsessions-v1-Capabilities)
    - [ControllerFrame](#agentsessions-v1-ControllerFrame)
    - [DescribeRequest](#agentsessions-v1-DescribeRequest)
    - [HarnessDescriptor](#agentsessions-v1-HarnessDescriptor)
    - [IdentityContext](#agentsessions-v1-IdentityContext)
    - [ModelResult](#agentsessions-v1-ModelResult)
    - [Start](#agentsessions-v1-Start)
    - [ToolSpec](#agentsessions-v1-ToolSpec)
  
    - [Resumability](#agentsessions-v1-Resumability)
  
    - [Harness](#agentsessions-v1-Harness)
  
- [session.proto](#session-proto)
    - [CancelRequest](#agentsessions-v1-CancelRequest)
    - [ComputeRef](#agentsessions-v1-ComputeRef)
    - [ComputeRef.AttributesEntry](#agentsessions-v1-ComputeRef-AttributesEntry)
    - [CreateSessionRequest](#agentsessions-v1-CreateSessionRequest)
    - [DeleteSessionRequest](#agentsessions-v1-DeleteSessionRequest)
    - [Delta](#agentsessions-v1-Delta)
    - [ExecRequest](#agentsessions-v1-ExecRequest)
    - [ExecUpdate](#agentsessions-v1-ExecUpdate)
    - [ForkRequest](#agentsessions-v1-ForkRequest)
    - [ForkRequest.LabelsEntry](#agentsessions-v1-ForkRequest-LabelsEntry)
    - [ForkResponse](#agentsessions-v1-ForkResponse)
    - [GetSessionRequest](#agentsessions-v1-GetSessionRequest)
    - [ListSessionsRequest](#agentsessions-v1-ListSessionsRequest)
    - [ListSessionsResponse](#agentsessions-v1-ListSessionsResponse)
    - [ReplayRequest](#agentsessions-v1-ReplayRequest)
    - [ResumeRequest](#agentsessions-v1-ResumeRequest)
    - [RuntimeCapabilities](#agentsessions-v1-RuntimeCapabilities)
    - [Session](#agentsessions-v1-Session)
    - [Session.AnnotationsEntry](#agentsessions-v1-Session-AnnotationsEntry)
    - [Session.LabelsEntry](#agentsessions-v1-Session-LabelsEntry)
    - [SnapshotRef](#agentsessions-v1-SnapshotRef)
    - [SuspendRequest](#agentsessions-v1-SuspendRequest)
  
    - [ComputeState](#agentsessions-v1-ComputeState)
    - [ExecState](#agentsessions-v1-ExecState)
  
    - [Sessions](#agentsessions-v1-Sessions)
  
- [Scalar Value Types](#scalar-value-types)



<a name="common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## common.proto
Shared wire types for the agentsessions Session and Harness services.

The Event message is used by BOTH the session log (Sessions.Exec / Replay) and the
harness stream (Harness.Connect) — the harness&#39;s events become the session log.

The content model (Part) is aligned with A2A `Part` &#43; MCP content for interop; the
event/log model is native. Derived from the durable-log &amp; replay contract §7 (record
schema) and §8 (interop). See agentsessions-replay-determinism-contract.md.


<a name="agentsessions-v1-ApprovalRequest"></a>

### ApprovalRequest
Approval is modeled as durable log entries: an unresolved request, later resolved by a
decision — so compute can suspend while awaiting a human/policy decision (§7).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tool_call_id | [string](#string) |  |  |
| reason | [string](#string) |  |  |






<a name="agentsessions-v1-ApprovalResult"></a>

### ApprovalResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tool_call_id | [string](#string) |  |  |
| approved | [bool](#bool) |  |  |
| reason | [string](#string) |  |  |






<a name="agentsessions-v1-DataPart"></a>

### DataPart
DataPart is structured JSON (A2A data part; MCP structuredContent).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |






<a name="agentsessions-v1-Error"></a>

### Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [int32](#int32) |  | grpc status code |
| description | [string](#string) |  |  |






<a name="agentsessions-v1-Event"></a>

### Event
Event is the harness-emitted content unit. Ordering (seq) and integrity (prev_hash /
content_hash) are host-assigned and live on LogRecord — the harness, which does not know
seq/prev, emits a hash-free Event, so there is no circular hashing. Streaming deltas are
transport (see Delta in session.proto) and coalesce into the finalized event (§3).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  | which execution/turn produced this event |
| ts | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| schema_version | [int32](#int32) |  | §7: versions the body; replay survives schema skew |
| kind | [EventKind](#agentsessions-v1-EventKind) |  |  |
| message | [Message](#agentsessions-v1-Message) |  |  |
| model | [ModelCall](#agentsessions-v1-ModelCall) |  |  |
| tool | [ToolCall](#agentsessions-v1-ToolCall) |  |  |
| result | [ToolResult](#agentsessions-v1-ToolResult) |  |  |
| approval | [ApprovalRequest](#agentsessions-v1-ApprovalRequest) |  |  |
| approval_result | [ApprovalResult](#agentsessions-v1-ApprovalResult) |  |  |
| usage | [Usage](#agentsessions-v1-Usage) |  |  |
| lifecycle | [Lifecycle](#agentsessions-v1-Lifecycle) |  |  |
| end | [HarnessEnd](#agentsessions-v1-HarnessEnd) |  |  |
| error | [Error](#agentsessions-v1-Error) |  |  |
| actor | [IdentityRef](#agentsessions-v1-IdentityRef) |  | emitter principal -&gt; provenance |






<a name="agentsessions-v1-FilePart"></a>

### FilePart
FilePart carries binary/media content inline or by reference (A2A file part; MCP
resource_link / embedded resource). mime is an IANA media type.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mime | [string](#string) |  |  |
| bytes | [bytes](#bytes) |  |  |
| uri | [string](#string) |  |  |
| name | [string](#string) |  | optional display name |
| digest | [string](#string) |  | content digest of the uri target (chain covers externalized bytes) |






<a name="agentsessions-v1-HarnessEnd"></a>

### HarnessEnd



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [string](#string) |  | COMPLETED | FAILED | CANCELED |
| error | [Error](#agentsessions-v1-Error) |  |  |






<a name="agentsessions-v1-IdentityRef"></a>

### IdentityRef
IdentityRef is the bound agent principal. OIDC-neutral: an Entra Agent ID is one issuer;
a SPIFFE ID or a GitHub App identity are others. Carried on every event.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| principal | [string](#string) |  |  |
| issuer | [string](#string) |  |  |
| subject | [string](#string) |  | session-scoped sub |






<a name="agentsessions-v1-Lifecycle"></a>

### Lifecycle
Lifecycle marks a compute/session transition in the log (§7). BASELINE is a replay /
compaction checkpoint (§1).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [Lifecycle.Kind](#agentsessions-v1-Lifecycle-Kind) |  |  |
| detail | [string](#string) |  | e.g. &#34;fork parent=&lt;uid&gt;@&lt;seq&gt;&#34;, baseline id |
| snapshot_local | [string](#string) |  | SnapshotRef captured on SUSPEND (§5.1), carried inline (mirroring the canonical api.SnapshotRef) so it rides the tamper-evident chain with no side table and no cross-file message dependency. This is distinct from the aspirational session-status message SnapshotRef in session.proto. A filesystem-only backend sets only snapshot_local (the session handle to replay) with snapshot_memory=false; a memory backend also sets snapshot_external_uri. |
| snapshot_external_uri | [string](#string) |  |  |
| snapshot_memory | [bool](#bool) |  |  |
| snapshot_sealed | [bool](#bool) |  |  |






<a name="agentsessions-v1-LogRecord"></a>

### LogRecord
LogRecord is the host&#39;s durable, ordered wrapper around an Event. The host assigns seq,
links the hash-chain (content_hash = H(prev_hash || seq || canonical(event)); at a fork
child.first.prev_hash = parent@R.content_hash), and records the incarnation fence token.
Externalized payloads (FilePart.uri, ReasoningPart.opaque_uri, ToolResult.output_uri)
carry their own content digest so the chain covers them by reference.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| seq | [int64](#int64) |  |  |
| prev_hash | [string](#string) |  |  |
| content_hash | [string](#string) |  |  |
| fence | [int64](#int64) |  |  |
| event | [Event](#agentsessions-v1-Event) |  |  |






<a name="agentsessions-v1-Message"></a>

### Message
Message is a role-tagged sequence of content parts (A2A Message = role &#43; Part[]).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) |  | user | assistant | tool |
| parts | [Part](#agentsessions-v1-Part) | repeated |  |






<a name="agentsessions-v1-ModelCall"></a>

### ModelCall
ModelCall is a model REQUEST emitted by the harness (EVENT_MODEL_CALL). The host records
it with input_hash and answers via ControllerFrame.ModelResult (correlated by id). The
completion (text &#43; reasoning parts) is recorded separately as an EVENT_OUTPUT message.
Under STATELESS_REPLAY the harness never calls a provider directly.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model | [string](#string) |  |  |
| params | [ModelCall.ParamsEntry](#agentsessions-v1-ModelCall-ParamsEntry) | repeated |  |
| input_hash | [string](#string) |  | §7/§9.1: required for STATELESS_REPLAY so the I0 check can run |
| id | [string](#string) |  | correlation id for the served ModelResult |
| messages | [Message](#agentsessions-v1-Message) | repeated | messages is the request context the host needs to INVOKE the model on the live path. It is wire-only: the host records the call with input_hash alone (messages stripped), since the recorded form only needs the hash to re-check I0 on replay. |






<a name="agentsessions-v1-ModelCall-ParamsEntry"></a>

### ModelCall.ParamsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-Origin"></a>

### Origin
Origin is the neutral external context a session is tied to. Producer adapters
(GitHub, Foundry, custom apps) fill it. Modeled on CloudEvents source/subject.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [string](#string) |  |  |
| subject | [string](#string) |  |  |
| uri | [string](#string) |  |  |
| attributes | [Origin.AttributesEntry](#agentsessions-v1-Origin-AttributesEntry) | repeated |  |






<a name="agentsessions-v1-Origin-AttributesEntry"></a>

### Origin.AttributesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-Part"></a>

### Part
Part is one unit of message content. Text, file (inline or URI), structured data, or an
opaque reasoning block. Large payloads are externalized by URI, never inlined (§8).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| text | [TextPart](#agentsessions-v1-TextPart) |  |  |
| file | [FilePart](#agentsessions-v1-FilePart) |  |  |
| data | [DataPart](#agentsessions-v1-DataPart) |  |  |
| reasoning | [ReasoningPart](#agentsessions-v1-ReasoningPart) |  |  |






<a name="agentsessions-v1-ReasoningPart"></a>

### ReasoningPart
ReasoningPart is an opaque, provider-tagged reasoning block that MUST be replayed
verbatim to preserve reasoning continuity within a turn (I2). agentsessions never
interprets `opaque` — all provider specifics live inside it. Verified across
Anthropic/OpenAI/Gemini in the Track A reasoning-continuity spike: one part type, no
per-provider sub-shapes, and no memory-snapshot edge (stateless verbatim replay).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| provider | [string](#string) |  | &#34;anthropic&#34; | &#34;openai&#34; | &#34;google&#34; — routing &#43; validation |
| model_id | [string](#string) |  | model that produced it (cross-model resend case) |
| opaque_bytes | [bytes](#bytes) |  |  |
| opaque_uri | [string](#string) |  | externalized when large; retained for the turn&#39;s fork lifetime |
| summary | [Part](#agentsessions-v1-Part) | repeated | optional, NON-authoritative, compaction-droppable (may be multimodal) |
| item_id | [string](#string) |  | optional provider id (e.g. OpenAI rs_...) for dedup/order |
| ordinal | [int32](#int32) |  | position within the turn relative to text/tool parts |
| validity_scope | [string](#string) |  | = execution_id (turn); see replay contract §5 |
| opaque_digest | [string](#string) |  | content digest of opaque_uri target (chain covers externalized bytes) |






<a name="agentsessions-v1-ResourceMetadata"></a>

### ResourceMetadata
ResourceMetadata is carried by every top-level resource.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| project | [string](#string) |  | tenant / namespace |
| name | [string](#string) |  |  |
| uid | [string](#string) |  |  |
| version | [int64](#int64) |  |  |
| create_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| update_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="agentsessions-v1-TextPart"></a>

### TextPart



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| text | [string](#string) |  |  |






<a name="agentsessions-v1-ToolCall"></a>

### ToolCall
ToolCall aligns with an MCP tool call (name &#43; structured args).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| tool | [string](#string) |  | tool name / MCP method |
| args | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
| mediation | [Mediation](#agentsessions-v1-Mediation) |  |  |
| idempotency_key | [string](#string) |  | §3/I3: dedups a retried side-effecting call tool-side |






<a name="agentsessions-v1-ToolResult"></a>

### ToolResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| output | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
| output_uri | [string](#string) |  | externalized large output (MCP resource_link), else inline `output` |
| output_digest | [string](#string) |  | content digest of output_uri target (chain covers externalized bytes) |
| is_error | [bool](#bool) |  |  |
| error | [string](#string) |  |  |






<a name="agentsessions-v1-Usage"></a>

### Usage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model | [string](#string) |  |  |
| input_tokens | [int64](#int64) |  |  |
| output_tokens | [int64](#int64) |  |  |
| reasoning_tokens | [int64](#int64) |  |  |





 


<a name="agentsessions-v1-EventKind"></a>

### EventKind


| Name | Number | Description |
| ---- | ------ | ----------- |
| EVENT_KIND_UNSPECIFIED | 0 |  |
| EVENT_INPUT | 1 | a user/producer message |
| EVENT_MODEL_CALL | 2 | model-call metadata (&#43; input_hash); output is an OUTPUT message |
| EVENT_OUTPUT | 3 | a finalized assistant message; streaming deltas are transport, not logged |
| EVENT_TOOL_CALL | 4 |  |
| EVENT_TOOL_RESULT | 5 |  |
| EVENT_APPROVAL_REQUEST | 6 |  |
| EVENT_APPROVAL_RESULT | 7 |  |
| EVENT_USAGE | 8 |  |
| EVENT_LIFECYCLE | 9 |  |
| EVENT_END | 10 |  |
| EVENT_ERROR | 11 |  |



<a name="agentsessions-v1-Lifecycle-Kind"></a>

### Lifecycle.Kind


| Name | Number | Description |
| ---- | ------ | ----------- |
| LIFECYCLE_UNSPECIFIED | 0 |  |
| LIFECYCLE_SUSPEND | 1 |  |
| LIFECYCLE_RESUME | 2 |  |
| LIFECYCLE_FORK | 3 |  |
| LIFECYCLE_BASELINE | 4 |  |
| LIFECYCLE_CANCEL | 5 |  |



<a name="agentsessions-v1-Mediation"></a>

### Mediation
Mediation controls how a tool call is executed / who enforces record-before-effect (§3).

| Name | Number | Description |
| ---- | ------ | ----------- |
| MEDIATION_UNSPECIFIED | 0 |  |
| MEDIATION_IN_HARNESS_REPORTED | 1 | harness runs it, reports for audit (cooperative WAL) |
| MEDIATION_CONTROLLER_MEDIATED | 2 | host/gateway executes (host-enforced WAL; trustless) |
| MEDIATION_REQUIRES_APPROVAL | 3 | pause for human/policy approval |


 

 

 



<a name="harness-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## harness.proto
The Harness BYOH SPI: the contract between the host and an agent implementation.

A Microsoft Agent Framework (MAF) agent, a GitHub Copilot agent, or a custom agent
implements it via a thin adapter. The host drives one execution per Connect stream;
the harness streams typed Events terminated by one EVENT_END. Beyond plain streaming, it
adds tool-call / approval mediation, per-call usage, and capability declaration.


<a name="agentsessions-v1-Cancel"></a>

### Cancel



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reason | [string](#string) |  | USER | TIMEOUT | INTERNAL |






<a name="agentsessions-v1-Capabilities"></a>

### Capabilities



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resumability | [Resumability](#agentsessions-v1-Resumability) |  |  |
| fork_safe | [bool](#bool) |  | no un-replayable side effects mid-turn |
| requires_gpu | [bool](#bool) |  |  |
| streaming | [bool](#bool) |  |  |
| reasoning_replay | [bool](#bool) |  | reasoning_replay: harness replays opaque reasoning parts verbatim via the provider stateless path; keeps reasoning on STATELESS_REPLAY (no memory-snapshot edge). |






<a name="agentsessions-v1-ControllerFrame"></a>

### ControllerFrame
ControllerFrame is sent by the host to the harness during an execution.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| execution_id | [string](#string) |  |  |
| start | [Start](#agentsessions-v1-Start) |  |  |
| cancel | [Cancel](#agentsessions-v1-Cancel) |  |  |
| approval | [ApprovalResult](#agentsessions-v1-ApprovalResult) |  | response to an EVENT_APPROVAL_REQUEST |
| tool | [ToolResult](#agentsessions-v1-ToolResult) |  | result for a CONTROLLER_MEDIATED tool call |
| model | [ModelResult](#agentsessions-v1-ModelResult) |  | served completion for a mediated/replayed model call |






<a name="agentsessions-v1-DescribeRequest"></a>

### DescribeRequest







<a name="agentsessions-v1-HarnessDescriptor"></a>

### HarnessDescriptor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| models | [string](#string) | repeated | model-agnostic: supported/required model ids |
| tools | [ToolSpec](#agentsessions-v1-ToolSpec) | repeated |  |
| capabilities | [Capabilities](#agentsessions-v1-Capabilities) |  |  |






<a name="agentsessions-v1-IdentityContext"></a>

### IdentityContext
IdentityContext carries the principal and signals whether the harness may mint
scoped, delegated tokens (Transaction Tokens) via the host for tool calls.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| principal | [IdentityRef](#agentsessions-v1-IdentityRef) |  |  |
| can_mint_tokens | [bool](#bool) |  |  |






<a name="agentsessions-v1-ModelResult"></a>

### ModelResult
ModelResult is a model completion served to the harness (live invocation or replay).
The Message carries text &#43; opaque reasoning parts, recorded verbatim for continuity.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [Message](#agentsessions-v1-Message) |  |  |
| usage | [Usage](#agentsessions-v1-Usage) |  |  |
| model_call_id | [string](#string) |  | correlates to the emitted ModelCall.id |






<a name="agentsessions-v1-Start"></a>

### Start
Start begins one execution. History is pushed here (pull-by-range is a later
addition); it is empty when the sandbox was memory-restored.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [bytes](#bytes) |  |  |
| history | [Event](#agentsessions-v1-Event) | repeated |  |
| inputs | [Message](#agentsessions-v1-Message) | repeated | New input messages for this execution. Empty = resume/re-drive an interrupted execution from history with no new input. |
| identity | [IdentityContext](#agentsessions-v1-IdentityContext) |  |  |
| resume_from_seq | [int64](#int64) |  |  |






<a name="agentsessions-v1-ToolSpec"></a>

### ToolSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| description | [string](#string) |  |  |
| mediation | [Mediation](#agentsessions-v1-Mediation) |  | default mediation for this tool |





 


<a name="agentsessions-v1-Resumability"></a>

### Resumability
Resumability declares how a harness survives suspend/resume/fork. The host matches
this against a runtime&#39;s RuntimeCapabilities.

| Name | Number | Description |
| ---- | ------ | ----------- |
| RESUMABILITY_UNSPECIFIED | 0 |  |
| RESUMABILITY_STATELESS_REPLAY | 1 | default: rehydrate from history; any runtime |
| RESUMABILITY_REQUIRES_MEMORY_SNAPSHOT | 2 | needs a memory-snapshot-capable runtime |


 

 


<a name="agentsessions-v1-Harness"></a>

### Harness


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Describe | [DescribeRequest](#agentsessions-v1-DescribeRequest) | [HarnessDescriptor](#agentsessions-v1-HarnessDescriptor) | Describe returns the static contract; used to match the harness to a runtime. |
| Connect | [ControllerFrame](#agentsessions-v1-ControllerFrame) stream | [Event](#agentsessions-v1-Event) stream | Connect drives ONE execution. The host sends Start (and optional control frames); the harness streams Events terminated by exactly one EVENT_END. |

 



<a name="session-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## session.proto
The Sessions control-plane API (client-facing) and the Runtime compute types.

A Session is a durable conversation (event log) plus zero-or-one live incarnation.
Each Exec is one execution/turn. Fork branches the log at a sequence.


<a name="agentsessions-v1-CancelRequest"></a>

### CancelRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| execution_id | [string](#string) |  |  |
| reason | [string](#string) |  | USER | TIMEOUT | INTERNAL |






<a name="agentsessions-v1-ComputeRef"></a>

### ComputeRef



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| runtime | [string](#string) |  | &#34;pod&#34; | &#34;kata&#34; | &#34;clh&#34; | &#34;substrate&#34; |
| worker | [string](#string) |  | pod/worker id when live/warm; empty when cold |
| snapshot | [SnapshotRef](#agentsessions-v1-SnapshotRef) |  |  |
| capabilities | [RuntimeCapabilities](#agentsessions-v1-RuntimeCapabilities) |  |  |
| attributes | [ComputeRef.AttributesEntry](#agentsessions-v1-ComputeRef-AttributesEntry) | repeated | backend-specific, e.g. substrate actor/atespace |
| fence_token | [int64](#int64) |  | monotonic; log rejects appends from a superseded incarnation |






<a name="agentsessions-v1-ComputeRef-AttributesEntry"></a>

### ComputeRef.AttributesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-CreateSessionRequest"></a>

### CreateSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [Session](#agentsessions-v1-Session) |  |  |






<a name="agentsessions-v1-DeleteSessionRequest"></a>

### DeleteSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uid | [string](#string) |  |  |






<a name="agentsessions-v1-Delta"></a>

### Delta
Delta is an ephemeral streaming chunk that coalesces into a finalized EVENT_OUTPUT (or
reasoning) part. No seq; never hash-chained (§8, A2A TaskArtifactUpdateEvent).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| part_index | [int32](#int32) |  | which content part it accumulates into |
| chunk | [string](#string) |  | text chunk; binary/media deltas are a later addition |
| done | [bool](#bool) |  | last delta for this part |






<a name="agentsessions-v1-ExecRequest"></a>

### ExecRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| inputs | [Message](#agentsessions-v1-Message) | repeated | Input messages for this turn. Empty = resume/re-drive the last non-terminal execution with no new input (recovery after a crash/interruption). |
| resume_from_seq | [int64](#int64) |  | client replay cursor after a disconnect (was: last_seq) |
| harness | [string](#string) |  | empty = session default |
| config | [bytes](#bytes) |  |  |
| expected_last_seq | [int64](#int64) |  | Single-writer CAS: the host commits this execution&#39;s first event only if the log head equals expected_last_seq. Mismatch is rejected (another writer advanced the log). |
| deadline_unix | [int64](#int64) |  | optional execution deadline (unix seconds); host cancels past it |






<a name="agentsessions-v1-ExecUpdate"></a>

### ExecUpdate
ExecUpdate is what the live Exec stream carries: a committed LogRecord, or an ephemeral
streaming Delta (transport only — not logged, not hash-chained).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| record | [LogRecord](#agentsessions-v1-LogRecord) |  |  |
| delta | [Delta](#agentsessions-v1-Delta) |  |  |






<a name="agentsessions-v1-ForkRequest"></a>

### ForkRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| at_seq | [int64](#int64) |  | 0 = HEAD. A REQUIRES_MEMORY_SNAPSHOT harness forks by cloning a memory snapshot, which captures RAM as of now, so it accepts only HEAD; a historical seq is FAILED_PRECONDITION. |
| count | [int32](#int32) |  | fan-out N children; must be 1..128, else INVALID_ARGUMENT |
| identity | [IdentityRef](#agentsessions-v1-IdentityRef) |  | optional child principal |
| labels | [ForkRequest.LabelsEntry](#agentsessions-v1-ForkRequest-LabelsEntry) | repeated |  |
| child_names | [string](#string) | repeated | Display names for the children, in the order they are returned. Length must be 0 or exactly count, else INVALID_ARGUMENT.

Left unset, a child&#39;s name is empty rather than a copy of the parent&#39;s. A fork inherits the parent&#39;s workload (project, harness, model) because that is what defines the run, but a name is a caller-supplied label the server has no basis to invent. Copying it makes a listing report N&#43;1 rows that claim to be the same session, and synthesizing one (&#34;&lt;parent&gt; fork 2&#34;) would put a presentation convention in the wire contract and be wrong the moment the same parent is forked by two separate calls. Lineage is already on the child as parent_uid and fork_seq, so a caller that wants a derived label can render one without the server denormalizing it into a string. |






<a name="agentsessions-v1-ForkRequest-LabelsEntry"></a>

### ForkRequest.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-ForkResponse"></a>

### ForkResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| children | [Session](#agentsessions-v1-Session) | repeated |  |






<a name="agentsessions-v1-GetSessionRequest"></a>

### GetSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uid | [string](#string) |  |  |






<a name="agentsessions-v1-ListSessionsRequest"></a>

### ListSessionsRequest
ListSessions enumerates sessions from the durable store, so it returns sessions that have no
events and no live incarnation yet. Paging is keyset over (create_time DESC, uid ASC), which is
stable when a session is created mid-pagination; an offset would skip or repeat rows.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| project | [string](#string) |  | tenant / namespace; exact-match filter, not a prefix or wildcard |
| page_size | [int32](#int32) |  | Maximum sessions to return. 0 = the server default; a value above the server maximum is clamped rather than rejected. |
| page_token | [string](#string) |  | Opaque cursor taken verbatim from a prior ListSessionsResponse.next_page_token. Empty = first page. A token the server cannot parse is INVALID_ARGUMENT, never a silent restart from page 1. |






<a name="agentsessions-v1-ListSessionsResponse"></a>

### ListSessionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessions | [Session](#agentsessions-v1-Session) | repeated |  |
| next_page_token | [string](#string) |  | empty on the last page |






<a name="agentsessions-v1-ReplayRequest"></a>

### ReplayRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| from_seq | [int64](#int64) |  |  |
| to_seq | [int64](#int64) |  | 0 = to head |






<a name="agentsessions-v1-ResumeRequest"></a>

### ResumeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |
| boot | [bool](#bool) |  | true = cold-boot &#43; replay instead of snapshot restore |






<a name="agentsessions-v1-RuntimeCapabilities"></a>

### RuntimeCapabilities



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| memory_snapshot | [bool](#bool) |  |  |
| cow_fork | [bool](#bool) |  |  |
| attest | [bool](#bool) |  |  |
| gpu_state | [bool](#bool) |  |  |






<a name="agentsessions-v1-Session"></a>

### Session



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [ResourceMetadata](#agentsessions-v1-ResourceMetadata) |  |  |
| harness | [string](#string) |  |  |
| model | [string](#string) |  | model-agnostic id |
| exec_state | [ExecState](#agentsessions-v1-ExecState) |  | execution/turn axis |
| compute_state | [ComputeState](#agentsessions-v1-ComputeState) |  | incarnation axis |
| last_seq | [int64](#int64) |  | event-log cursor |
| parent_uid | [string](#string) |  | fork lineage |
| fork_seq | [int64](#int64) |  |  |
| identity | [IdentityRef](#agentsessions-v1-IdentityRef) |  |  |
| compute | [ComputeRef](#agentsessions-v1-ComputeRef) |  | empty when SUSPENDED/TERMINATED |
| origin | [Origin](#agentsessions-v1-Origin) |  | neutral external context; filled by a producer adapter |
| labels | [Session.LabelsEntry](#agentsessions-v1-Session-LabelsEntry) | repeated |  |
| annotations | [Session.AnnotationsEntry](#agentsessions-v1-Session-AnnotationsEntry) | repeated | free-form adapter metadata (K8s-style) |






<a name="agentsessions-v1-Session-AnnotationsEntry"></a>

### Session.AnnotationsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-Session-LabelsEntry"></a>

### Session.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="agentsessions-v1-SnapshotRef"></a>

### SnapshotRef
SnapshotRef is the session-STATUS ref carried in ComputeRef below. Its shape mirrors the canonical
Go SPI type api.SnapshotRef and the durable form the log carries inline on the SUSPEND Lifecycle
event (common.proto Lifecycle.snapshot_*), so the three agree field for field.

local and external_uri are deliberately NOT a oneof: a memory-capable backend sets both at once
(substrate reports an actor handle in local and the object-storage URI in external_uri), which a
oneof cannot express. A filesystem-only backend sets only local, with memory=false.

ComputeRef is not yet populated on the wire; the Placer records the ref on the SUSPEND event
instead. Wiring session status is tracked separately.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| local | [string](#string) |  |  |
| external_uri | [string](#string) |  |  |
| memory | [bool](#bool) |  | RAM/process captured? pod: false, kata/clh: true |
| sealed | [bool](#bool) |  | encrypted &#43; attested (confidential) |






<a name="agentsessions-v1-SuspendRequest"></a>

### SuspendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session | [string](#string) |  |  |





 


<a name="agentsessions-v1-ComputeState"></a>

### ComputeState


| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPUTE_STATE_UNSPECIFIED | 0 |  |
| COMPUTE_NONE | 1 | no incarnation |
| COMPUTE_LIVE | 2 |  |
| COMPUTE_WARM | 3 | paused: resident, worker held |
| COMPUTE_COLD | 4 | suspended: snapshot in storage, worker freed |
| COMPUTE_TERMINATED | 5 |  |



<a name="agentsessions-v1-ExecState"></a>

### ExecState
ExecState is the execution/turn axis; ComputeState is the incarnation axis. The two are
modeled explicitly (§6 of the replay contract) and reported independently: there is no
flattened lifecycle enum, because collapsing them loses which axis actually moved.

| Name | Number | Description |
| ---- | ------ | ----------- |
| EXEC_STATE_UNSPECIFIED | 0 |  |
| EXEC_PENDING | 1 |  |
| EXEC_RUNNING | 2 |  |
| EXEC_AWAITING | 3 | blocked on approval/input, resolved from the log |
| EXEC_COMPLETED | 4 |  |
| EXEC_FAILED | 5 |  |
| EXEC_CANCELED | 6 |  |


 

 


<a name="agentsessions-v1-Sessions"></a>

### Sessions


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateSession | [CreateSessionRequest](#agentsessions-v1-CreateSessionRequest) | [Session](#agentsessions-v1-Session) |  |
| GetSession | [GetSessionRequest](#agentsessions-v1-GetSessionRequest) | [Session](#agentsessions-v1-Session) |  |
| ListSessions | [ListSessionsRequest](#agentsessions-v1-ListSessionsRequest) | [ListSessionsResponse](#agentsessions-v1-ListSessionsResponse) |  |
| DeleteSession | [DeleteSessionRequest](#agentsessions-v1-DeleteSessionRequest) | [Session](#agentsessions-v1-Session) |  |
| Exec | [ExecRequest](#agentsessions-v1-ExecRequest) | [ExecUpdate](#agentsessions-v1-ExecUpdate) stream | Exec runs one execution/turn. The live stream carries committed LogRecords plus ephemeral Deltas; Replay re-delivers committed records only (read-only). |
| Replay | [ReplayRequest](#agentsessions-v1-ReplayRequest) | [LogRecord](#agentsessions-v1-LogRecord) stream |  |
| Cancel | [CancelRequest](#agentsessions-v1-CancelRequest) | [Session](#agentsessions-v1-Session) | In-flight control.

cancel the in-flight execution |
| Suspend | [SuspendRequest](#agentsessions-v1-SuspendRequest) | [Session](#agentsessions-v1-Session) | Compute-layer durability. There is no warm Pause: no Runtime backend implements a node-local warm checkpoint, so a session goes straight from live to a cold snapshot.

cold, free worker |
| Resume | [ResumeRequest](#agentsessions-v1-ResumeRequest) | [Session](#agentsessions-v1-Session) |  |
| Fork | [ForkRequest](#agentsessions-v1-ForkRequest) | [ForkResponse](#agentsessions-v1-ForkResponse) | The differentiator: branch a session at a sequence into one or more children. Forking a REQUIRES_MEMORY_SNAPSHOT harness first checkpoints the parent (it suspends, and a SUSPEND event lands on its chain) because the children are cloned from that snapshot; resume brings it back. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

