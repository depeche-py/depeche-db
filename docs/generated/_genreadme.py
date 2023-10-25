with open("../../README.md", "w") as dest, open("README.md") as src, open(
    "../../examples/readme.py"
) as example:
    dest.write(src.read().replace("%EXAMPLE%", example.read()))
