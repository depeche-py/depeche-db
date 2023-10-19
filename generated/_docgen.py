import contextlib
import importlib
import inspect
import pathlib
import re
import sys
import textwrap
from typing import Dict, List, Union

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.absolute()))


class Code:
    def __init__(self, code):
        self.code = code
        self.outputs = []

    def render(self):
        if not self.code and not self.outputs:
            return ""
        return textwrap.dedent(
            """\
            ```python
            {code}{delimiter}{outputs}
            ```
            """
        ).format(
            code=self.code,
            delimiter="\n" if self.outputs else "",
            outputs="\n".join([output.output for output in self.outputs]),
        )


class Markdown:
    def __init__(self, markdown):
        self.markdown = markdown
        self.code_outputs = []

    def add_code_output(self, code_output):
        self.code_outputs.append(code_output)

    def render(self):
        return self.markdown.strip(" ")


class CodeOutput:
    def __init__(self, output):
        self.output = output


Part = Union[Code, Markdown]


class DocGen:
    def __init__(self, input_file_name):
        self.input_file_name = input_file_name
        self.markdown_parts: Dict[str, List[Markdown]] = {}
        self.current_file = None
        self.current_show = None
        self.current_show_caller = None

    @staticmethod
    def run(input_file_name):
        mod = importlib.import_module(input_file_name[:-3])
        doc = mod.doc
        code = doc.get_code_parts()
        for output_file, content in doc.markdown_parts.items():
            code_parts = iter(code[output_file])
            interleaved: List[Part] = [next(code_parts)]
            for part in content:
                interleaved.append(part)
                code_part = next(code_parts)
                code_part.outputs = part.code_outputs
                interleaved.append(code_part)
            result = "\n".join(part.render() for part in interleaved)
            with open(f"output/{output_file}", "w") as f:
                f.write(result)

    def get_code_parts(self) -> Dict[str, List[Code]]:
        result: Dict[str, List[Code]] = {}
        code = open(self.input_file_name).read()
        file_parts = ["doc.output(" + part for part in code.split("doc.output(")[1:]]
        for file_part in file_parts:
            file_name = re.search(r"\"(.*)\"", file_part).group(1)
            result[file_name] = []
            file_part_clean: List[str] = []
            in_md = False
            in_catch = False
            for line in file_part.splitlines():
                if line.startswith("doc.output"):
                    continue
                if line.startswith("doc.md"):
                    in_md = True
                    continue
                if line.startswith("with doc.catch"):
                    in_catch = True
                    continue
                if in_catch:
                    if not line.startswith("    "):
                        in_catch = False
                    else:
                        line = line[4:]
                if in_md and line == ")":
                    in_md = False
                    result[file_name].append(Code("\n".join(file_part_clean).strip()))
                    file_part_clean = []
                    continue
                if in_md:
                    continue
                if line.startswith("# >"):
                    continue
                if line.startswith("doc.begin_show") or line.startswith("doc.end_show"):
                    continue
                if "doc.show" in line:
                    line = line.replace("doc.show", "print")
                file_part_clean.append(line)
            result[file_name].append(Code("\n".join(file_part_clean).strip()))
        return result

    def output(self, output_file):
        self.current_file = output_file
        self.markdown_parts[output_file] = []

    def md(self, markdown):
        self.markdown_parts[self.current_file].append(
            Markdown(textwrap.dedent(markdown))
        )

    def begin_show(self):
        self.current_show = []
        self.current_show_caller = inspect.stack()[1]

    def end_show(self):
        display = self.get_code_output(
            self.current_show_caller.filename, self.current_show_caller.lineno
        )
        self.markdown_parts[self.current_file][-1].add_code_output(CodeOutput(display))
        print("-" * 80)
        print(self.current_show_caller.filename, self.current_show_caller.lineno)
        print("\n".join(self.current_show))
        print("=" * 80)
        print(display)
        print("-" * 80)
        self.current_show = None
        self.current_show_caller = None

    def show(self, value):
        if self.current_show is not None:
            self.current_show.append(str(value))
            return
        caller = inspect.stack()[1]
        display = self.get_code_output(caller.filename, caller.lineno)
        self.markdown_parts[self.current_file][-1].add_code_output(CodeOutput(display))
        print("-" * 80)
        print(caller.filename, caller.lineno)
        print(value)
        print("=" * 80)
        print(display)
        print("-" * 80)

    def get_code_output(self, filename, lineno):
        display_lines = []
        with open(filename) as f:
            lines = f.readlines()
            found = False
            for line in lines[lineno - 1 :]:
                if found and not line.startswith("# >"):
                    break
                if line.startswith("# >"):
                    display_lines.append("# " + line[3:-1])
                    found = True
        return "\n".join(display_lines)

    @contextlib.contextmanager
    def catch(self):
        try:
            yield
        except Exception:
            pass


CURRENT = None


def main():
    assert len(sys.argv) == 2, "Usage: docgen.py <input file>"
    input_file = sys.argv[1]
    DocGen.run(input_file)


if __name__ == "__main__":
    main()
