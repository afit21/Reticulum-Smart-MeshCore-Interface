# Legacy simulated-mesh scenarios

Archived 2026-09-20. `test_sim_scenarios.py` ran two real interfaces through
`testscripts/simmesh`'s fake firmware model over simulated repeater hops. The
MeshBench scenario suite (`testscripts/meshbench_scenarios.py`) stages the
same field-diagnosed incidents against real MeshCore firmware and replaced it
as the fidelity tier. This directory is deliberately not a package, so
`python3 -m unittest discover -s tests` does not collect it; run
`python3 -m unittest tests.legacy.test_sim_scenarios` from the repo root to
reproduce an old result. New scenarios go in the MeshBench suite.
