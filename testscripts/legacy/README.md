# Legacy simulation scripts

Archived 2026-09-20. These drove the `testscripts/simmesh` fake-firmware mesh
as a fidelity tier between the unit tests and field tests:

- `fake_meshcore_repeater_sim.py` -- real interfaces over an in-process
  simulated mesh you define (`--link`, `--repeater`, loss models, `--profile`).
- `calibrate_sim_from_captures.py` -- per-hop stats from field captures and a
  suggested sim `--profile`.

Superseded by `testscripts/meshbench_scenarios.py`, which runs the interface
against real MeshCore firmware under MeshBench with only the RF modelled.
`simmesh` itself stays in `testscripts/` because the fast unit suite under
`tests/` uses its fake `meshcore` library. Both scripts still run from here
(they add `testscripts/` to `sys.path`), for reading old results only.
