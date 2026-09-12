# Bounded landing admission recovery

This operator recovery addresses TKT-nupis-dodiv-mosuv. A healthy verification holding the sole repository slot must not turn another candidate into a failed execution. Actual failures and execution timeouts remain fail closed.

## State and authority

A prepared landing freezes an admission start and deadline from the existing landing gate timeout. That deadline covers both shared cargo target serialization and verification admission, survives restart, and is separate from each named check execution timeout. Waiting produces durable candidate/check evidence and never spends the child-death retry allowance. The deadline is also bound to the kernel boot UUID and monotonic clock on Linux and macOS. Restart during a wait cannot refund elapsed time after a wall-clock adjustment. Missing clock evidence or a different boot expires the old allowance; an explicit operator retry grants a fresh bounded attempt.

If that finite allowance expires before execution, record an admission hold containing the full source generation, canonical task, repository, branch/head, target/base, prepared candidate, check, and never-executed result. Keep the prepared candidate referenced. Ordinary submissions remain deduplicated against the terminal processed marker.

An operator may explicitly retry an exact admission hold with a reason. A durable receipt is written before queue insertion and is the sole authority to supersede that hold for one bounded attempt. A repeated or restarted request resumes the same receipt. It never deletes processed markers or prior failure evidence. A changed identity, moved source/target, missing generation, legacy ambiguous failure, real check failure, execution timeout, or spent infrastructure retry cannot authorize this operation. Full named gates, review and target CAS still apply.

Fresh candidates using verification admission or shared cargo serialization are processed singly, so a batched hold cannot recreate the unchanged-target recovery dead end. Legacy already-prepared batches remain preserved and fail closed; they require normal recovery after a real target change.

The receipt carries the frozen new deadline and exact candidate into the ordinary durable queue. A new terminal result settles that receipt. Crash recovery must cover receipt-before-enqueue, queued-before-reply, and terminal-before-queue-removal. Historical receipts cannot supersede newer terminal results.

## Legacy invalid queue rows

Validate durable queued source identities before Git preparation, including batch admission. Proven cross-repository generation capture is a terminal quarantine with its complete original entry preserved. Missing Git access or another transient infrastructure error is not proof of an invalid source. Quarantine also retires prepared peers sharing a contaminated candidate, preserving their exact candidate refs and evidence across restart. Valid sources in that cohort require fresh gated resubmission. Quarantine never records delivery or resolves source tickets.

## Verification

Regressions hold verification admission beyond a cheap check execution deadline; release capacity within the outer allowance; expire the outer allowance without executing; retry the same source/target once; replay and restart requests; deny mismatched and ordinary failed holds; and retain candidate references through startup collection. A valid candidate behind a quarantined foreign row must still run normally.
