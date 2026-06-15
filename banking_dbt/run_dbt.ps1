# Run dbt inside the Airflow container (Python 3.12 + dbt already installed).
# Local `dbt` on Windows fails with Python 3.14 — use this script instead.

param(
    [Parameter(Position = 0)]
    [ValidateSet("debug", "run", "snapshot", "all")]
    [string]$Command = "all"
)

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."

function Invoke-DbtInAirflow {
    param([string]$DbtArgs)
    docker exec airflow-scheduler bash -c "cd /opt/airflow/banking_dbt && dbt $DbtArgs --profiles-dir /home/airflow/.dbt"
}

switch ($Command) {
    "debug"    { Invoke-DbtInAirflow "debug" }
    "run"      { Invoke-DbtInAirflow "run" }
    "snapshot" {
        Invoke-DbtInAirflow "snapshot"
        Invoke-DbtInAirflow "run --select marts"
    }
    "all"      {
        Invoke-DbtInAirflow "run"
        Invoke-DbtInAirflow "snapshot"
        Invoke-DbtInAirflow "run --select marts"
    }
}
