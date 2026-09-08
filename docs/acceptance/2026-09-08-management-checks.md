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
| Manual Script execution | Failed before Task publication | HTTP 500 on both `qa-initialize` and currently serving `app-api` |
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

Manual Scripts used only `test -r /var/www/html/artisan`; neither attempted Run
returned a Task id. Successful deployment pre-hooks are separate evidence and
do not prove this manual path. Runtime Script failure/abort behavior, new Secret
Entry consumption/replacement, and Volume persistence/removal are not proven.
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
