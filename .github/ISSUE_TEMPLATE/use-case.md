---
name: Workload or integration request
about: Describe a concrete batching workload and the backend contract it needs.
title: "[Use case] "
---

## Workload and backend

What are you batching, which client/API/version do you use, and which bulk operation is available?
Link the official API documentation when possible.

## Inputs, results, and compatible groups

Provide a small redacted example. Explain result ordering, missing/duplicate keys, and which
tenant, namespace, model, schema, or operation settings must stay separate. Do not include secrets
or private customer data.

## Capacity and latency

What record/byte/token limits does the backend enforce? What throughput and end-to-end latency
do you need, and what have you measured? Describe variable item sizes and burst behavior.

## Failure, cancellation, and replay

Can some writes succeed before an error? Are operations safe to retry? Who owns acknowledgements,
checkpoints, or deduplication after a crash? What should happen when a caller disconnects?

## Current approach and smallest useful change

Include a minimal example, relevant constructor settings, Python/package versions, and whether
you use a published release or a source commit. Which existing function/adapter/recipe did you try?
