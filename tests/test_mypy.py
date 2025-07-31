import pathlib as _pathlib
import subprocess as _subprocess
import sys as _sys


def test_typing():
    ini = "mypy.ini"
    if _sys.version_info < (3, 10):
        ini = "mypy39.ini"
    repo_path = _pathlib.Path(__file__).parent.parent
    res = _subprocess.run(
        [
            "uv",
            "run",
            "--",
            "mypy",
            "--config",
            str(repo_path / ini),
            ".",
            "--show-traceback",
        ],
        capture_output=True,
    )
    if res.returncode != 0:
        print(f"`{' '.join(res.args)}` exited with error: {res.returncode}")
        print(res.stdout.decode())
        print(res.stderr.decode())
        raise AssertionError("Mypy failed")
