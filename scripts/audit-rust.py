#!/usr/bin/env python3
from pathlib import Path
import json
import sys


def delimiter_error(text: str):
    stack = []
    pairs = {")": "(", "]": "[", "}": "{"}
    index = 0
    line = 1
    length = len(text)

    while index < length:
        char = text[index]
        following = text[index + 1] if index + 1 < length else ""

        if char == "\n":
            line += 1
            index += 1
            continue
        if char == "/" and following == "/":
            index += 2
            while index < length and text[index] != "\n":
                index += 1
            continue
        if char == "/" and following == "*":
            depth = 1
            index += 2
            while index < length and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    if text[index] == "\n":
                        line += 1
                    index += 1
            continue

        raw_prefix = 2 if text.startswith("br", index) else 1 if char == "r" else 0
        if raw_prefix:
            cursor = index + raw_prefix
            hashes = 0
            while cursor < length and text[cursor] == "#":
                hashes += 1
                cursor += 1
            if cursor < length and text[cursor] == '"':
                terminator = '"' + "#" * hashes
                end = text.find(terminator, cursor + 1)
                if end < 0:
                    return f"unterminated raw string at line {line}"
                line += text[index:end + len(terminator)].count("\n")
                index = end + len(terminator)
                continue

        if char == '"':
            index += 1
            while index < length:
                if text[index] == "\\":
                    index += 2
                    continue
                if text[index] == '"':
                    index += 1
                    break
                if text[index] == "\n":
                    line += 1
                index += 1
            continue

        if char == "'":
            # Lifetimes are not delimiters. Escaped or plain chars have a nearby
            # closing quote; all other apostrophes are left alone.
            if index + 2 < length and text[index + 2] == "'":
                index += 3
                continue
            if index + 3 < length and text[index + 1] == "\\" and text[index + 3] == "'":
                index += 4
                continue
            index += 1
            continue

        if char in "([{":
            stack.append((char, line))
            index += 1
            continue
        if char in ")]}":
            if not stack or stack[-1][0] != pairs[char]:
                return f"mismatched {char} at line {line}"
            stack.pop()
        index += 1

    if stack:
        return f"unclosed delimiters: {stack[-5:]}"
    return None


root = Path(__file__).resolve().parent.parent
files = sorted((root / "src-tauri" / "src").rglob("*.rs"))
failures = []
for path in files:
    error = delimiter_error(path.read_text())
    if error:
        failures.append(f"{path.relative_to(root)}: {error}")

print(json.dumps({"ok": not failures, "files": len(files), "failures": failures}, indent=2))
sys.exit(1 if failures else 0)
