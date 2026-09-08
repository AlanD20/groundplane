# QA deployment-management checks — 2026-09-08

Target: disposable QA `10.25.0.2`, tenant `entensy`, project `kobwnewe`,
Environment `qa-application`. Controller remains private. No production or
provider configuration changes, backup/restore work, or broad CI acceptance.

## Results

| Check | Result | Evidence |
| --- | --- | --- |
| Fresh group deployment | Passed | `task_01M20WZAF3SKAWTT51P2TME0EN`, completed 16:18:32 UTC |
| Distinguishable image deployment | Passed | `task_01M20Y7MSW373TMNHV2CMNR1H8`, completed 16:40:29 UTC |
| Explicit rollback preview and rollback | Passed | `task_01M20YD5V7MC42EB132PP4GS3Z`, completed 16:43:28 UTC |
| Public HTTPS and replicated WebSocket delivery | Passed after both image deployment and rollback | Both replicas, stable Reverb target, private router, public TLS Tunnel |
| Volume create/rename/removal | Blocked before create | Initial 500 corrected; rerun 422 retained-runtime coverage rejection |
| Project Secret creation and masked read | Passed | Disposable `sec_01M20YA9F0B5JZQ4S8A5RR64XW`; no value returned by show |
| Service-scoped Secret Entry consumption | Blocked before publication | 422 retained-runtime coverage rejection; no Entry created |
| Script creation and edit | Passed | Stable Script id retained; edit advanced generation from 1 to 2 |
| Manual Script execution | Repaired; success/failure/abort checked | Follow-up Tasks below; post-execution removal remains blocked |
| Disposable Script/Secret removal | Passed | Three removal Tasks completed; ids below |
| Application logs | Passed | Bounded `service logs app-api --tail 5` returned normally |

These findings do **not** establish complete deployment-management acceptance
or full MVP readiness. Existing application hosting remains operational.

## Image and rollback scope

The new tag `groundplane-rpi-app:qa-management-20260908` is a QA-only derivative
of the existing application image, not a new application feature build. It adds
the harmless `/opt/groundplane-qa-revision` marker and preserves the `www-data`
runtime user. All four logical app group members ran the new image
`sha256:a3e6d82cbf40575b930bee7ce25782d081a0614b683e52dd4a9a67f3eeb208e8`.
The API marker was read successfully. Both Reverb replicas were healthy with
zero restarts.

Rollback preview selected each member's earlier `qa-floor-20260908` Release.
Rollback returned API, Reverb, queue, and scheduler to image `023d784f3f53`;
the API marker was absent. Both Reverb replicas remained healthy with zero
restarts. Five-target delivery passed again, including certificate-verified
public TLS. Rolling back to the *current* tag was correctly rejected before
publication; the historical `dev` tag was not used because it predates the public
hostname configuration.

## Failed paths and safety

See [Volume/Entry retained-runtime blocker](../issues/volume-mixed-runtime-mutations.md).
The first narrow Volume correction has focused red/green race evidence and
passes the Volume package; it does not resolve the later mixed-runtime blocker.
The private previous Controller binary is retained as
`controller-before-volume-diagnostic`. All temporary diagnostics were removed
from source and the deployed Controller.

The initial manual Scripts used only `test -r /var/www/html/artisan`; neither
attempt returned a Task id. The publication repair and subsequent manual runtime
checks are recorded below. New Secret Entry consumption/replacement and Volume
persistence/removal remain unproven.
The known full-Blueprint publication-size problem also remains open; a full
bundle Apply was not repeated as part of this check.

## Test-resource cleanup

Only the newly created resources were removed through normal APIs:

- Edited app Script `scr_01M20YG8KFCW9G0ZKWEE0GZ46A`:
  `task_01M20YHEPD62KG0PGRSQ2VW8T2` completed.
- Initial Script `scr_01M20YCKRBT5217FXRQ6ZWG4RY`:
  `task_01M20YHESGTTAM6E1K6J5DEHJ0` completed.
- Unused Secret `sec_01M20YA9F0B5JZQ4S8A5RR64XW`:
  `task_01M20YHETKJRQZ35FCDDE5QH90` completed.

No test Volume or Entry was published. Existing application Volumes, Entries,
Secrets, and migration Scripts were retained. The harmless QA image tag remains
host-local so its immutable Release history stays usable.

Raw bounded Volume evidence and private log output are retained under
`.tmp/qa-management-20260908/`; the existing fanout verifier is
`.tmp/qa-fresh-20260908/reverb-public-hostname-fanout.php`.

## Manual Script publication repair — 17:13–17:16 UTC

Controller logs localized the original 500 to `idempotency plan contains a
duplicate compare key`. The Service desired-state fence and Environment
projection both compare the same Blueprint head. Manual publication now combines
identical source conditions once and rejects conflicting revisions. Generic
transaction duplicate-key validation is unchanged. Independent Service roots,
immutable projection roots, and concurrent head-change fences are retained.

The reproduction failed with that exact duplicate-key error before the fix.
Focused race tests passed for `TestManualScriptSourceConditions`, immutable
projection sources, and Script checkpoint evidence. The broader prefix selection
`^Test(ManualScript|Script|ReleaseScript|ReleaseHook)` also passed with race and
coverage collection. Tagged Controller build passed and was deployed to QA;
the prior binary is retained as `controller-before-manual-script-fix`.

Disposable Script `scr_01M2104ECJZ050QH7RMDSCFSWG`, slug
`qa-manual-cas-proof`, targeted the existing `app-api` Service:

| Body / action | Task | Terminal result (UTC) |
| --- | --- | --- |
| `test -r /var/www/html/artisan` | `task_01M2104XFFZEZH8EEQ6BEAQYY6` | completed 17:13:15 |
| Edit to `exit 7`, then Run | `task_01M2106S9M61XWETZ4SRS8NPXP` | failed 17:14:16 |
| Edit to `sleep 60`, Run, observe running, Abort | `task_01M210864YX5YA5GJ254TNVT4X` | aborted 17:15:26 |

The Script retained its id across generations 1–3. Normal removal then rejected
`resource.in_use: active Script executions fence deletion` despite all three
Tasks being terminal. The disposable Script is retained, not force-deleted;
see [manual Script terminal-reference cleanup](../issues/manual-script-terminal-references.md).
Both public HTTP endpoints remained 200 with TLS verification result 0.

A wider substring test selection additionally reached four failing subcases in
`TestAbortPendingBlueprintReleasesScriptExecutionAuthorityBeforeTerminalization`:
fixture publication returned `release durable record is corrupt`. All four
reproduced using unchanged main source via a Go overlay, excluding the new
manual-publication files. This is pre-existing fixture evidence, not a passing
full-package result. The overlay and coverage are retained under
`.tmp/qa-management-20260908/`. No full CI or backup/restore tests were run.
