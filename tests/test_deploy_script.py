import subprocess
from pathlib import Path


def test_deploy_script_has_valid_bash_syntax():
    path = Path(__file__).parents[1] / "scripts" / "deploy_prod.sh"
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_deploy_script_has_rollback_and_health_gate():
    text = (Path(__file__).parents[1] / "scripts" / "deploy_prod.sh").read_text()
    assert "trap rollback ERR" in text
    assert "State.Health.Status" in text
    assert "--no-deps" in text
