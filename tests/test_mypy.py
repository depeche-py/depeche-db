import pathlib as _pathlib
import subprocess as _subprocess


def test_typing():
    ini = "mypy.ini"
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
