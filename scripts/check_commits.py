#!/usr/bin/env python3
"""Check commit messages against the commit rule in CONTRIBUTING.md.

  check_commits.py --message-file=PATH    a message being written (the commit-msg hook)
  check_commits.py --subject=TEXT         one subject line, such as a pull request title
  check_commits.py --range=BEFORE..AFTER  the commits after BEFORE, up to and including AFTER

If BEFORE isn't a known commit, as on a branch's first push or after a force push, --range
checks only the newest commit and says so on stderr. Merge commits are skipped.

One " (#N)" at the end of a subject is ignored, because a squash merge on GitHub adds it.

--message-file removes comment lines and everything below the scissors, as git does after an
editor, using the comment prefix from git's config. Git sets GIT_EDITOR to ":" for its hooks
when no editor was opened, so then the text is checked as written.

Exit status: 0 passed, 1 a message breaks the rule, 2 the check couldn't run.
"""

import argparse
import os
import re
import subprocess
import sys

TYPES = ("build", "chore", "ci", "docs", "feat", "fix", "perf", "refactor", "revert", "style", "test")
MAX_SUBJECT = 72
SUBJECT = re.compile(r"(?P<type>[a-z]+)(?:\([a-z0-9][a-z0-9-]*\))?!?: \S.*")
GIT_REVERT = re.compile(r'(?:Revert|Reapply) ".+"')
PR_NUMBER = re.compile(r" \(#[0-9]+\)\Z")
# Git writes these itself, and they're fine while working on a branch. They're only
# accepted from the hook, so they can't reach the main branch through a pull request.
IN_PROGRESS = ("Merge ", "fixup! ", "squash! ", "amend! ")
SCISSORS = "------------------------ >8 ------------------------"


class CannotCheck(Exception):
    pass


def subject_problems(subject):
    # A squash merge on GitHub adds " (#N)", and that isn't part of the rule.
    title = PR_NUMBER.sub("", subject)
    if GIT_REVERT.fullmatch(title):
        return []
    match = SUBJECT.fullmatch(title)
    if not match:
        return [
            'expected "type(scope): summary" with a lowercase type and a scope of '
            f"a-z, 0-9 and - only, got {subject!r}"
        ]
    problems = []
    if match["type"] not in TYPES:
        problems.append(f'unknown type "{match["type"]}", use one of: {", ".join(TYPES)}')
    if len(title) > MAX_SUBJECT:
        problems.append(f"the subject is {len(title)} characters, the limit is {MAX_SUBJECT}")
    if title.endswith("."):
        problems.append("the subject shouldn't end with a full stop")
    return problems


def message_problems(message):
    lines = message.strip().splitlines()
    if not lines:
        return ["the message is empty"]
    problems = subject_problems(lines[0])
    if len(lines) > 1 and lines[1].strip():
        problems.append("leave a blank line between the subject and the body")
    return problems


def without_comments(text, comment="#"):
    """What git keeps of a message from the editor: no comments, nothing below the scissors."""
    kept = []
    for line in text.splitlines():
        if line == f"{comment} {SCISSORS}":
            break
        if not line.startswith(comment):
            kept.append(line)
    return "\n".join(kept)


def message_file_problems(text, editor_used=True, comment="#"):
    written = without_comments(text, comment) if editor_used else text
    # Git removes whitespace from the end of each line when it stores the message.
    message = "\n".join(line.rstrip(" \t\r") for line in written.splitlines()).strip()
    if message.startswith(IN_PROGRESS):
        return []
    # After an editor, git refuses an empty message itself and says why.
    if editor_used and not message:
        return []
    return message_problems(message)


def git(*args):
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )


def comment_prefix():
    for key in ("core.commentString", "core.commentChar"):
        value = git("config", "--get", key).stdout.rstrip("\n")
        if value:
            # With auto, git picks a character per message, which a hook can't see.
            return "#" if value.lower() == "auto" else value
    return "#"


def resolve(revision):
    result = git("rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}")
    return result.stdout.strip() if result.returncode == 0 else None


def commits_in(before, after):
    newest = resolve(after)
    if newest is None:
        raise CannotCheck(f"can't find commit {after!r}")
    oldest = resolve(before) if before else None
    selection = [f"{oldest}..{newest}"] if oldest else ["-1", newest]
    result = git("log", "--no-merges", "--format=%H%x00%B%x00", *selection, "--")
    if result.returncode != 0:
        raise CannotCheck(f"git log failed: {result.stderr.strip()}")
    if not oldest:
        note = f"'{before}' isn't a known commit, so only the newest commit was checked."
        print(note, file=sys.stderr)
    fields = result.stdout.split("\0")
    return [(sha.strip(), message.strip()) for sha, message in zip(fields[::2], fields[1::2])]


def range_problems(value):
    before, separator, after = value.partition("..")
    if not separator or not after or after.startswith("."):
        raise CannotCheck(f"expected BEFORE..AFTER, got {value!r}")
    commits = commits_in(before, after)
    print(f"Checked {len(commits)} commit(s).")
    return [
        f"{sha[:12]}: {problem}"
        for sha, message in commits
        for problem in message_problems(message)
    ]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check commit messages.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--message-file", metavar="PATH")
    mode.add_argument("--subject", metavar="TEXT")
    mode.add_argument("--range", metavar="BEFORE..AFTER")
    args = parser.parse_args(argv)
    try:
        if args.message_file is not None:
            with open(args.message_file, encoding="utf-8", errors="replace") as file:
                text = file.read()
            if os.environ.get("GIT_EDITOR") == ":":
                problems = message_file_problems(text, editor_used=False)
            else:
                problems = message_file_problems(text, comment=comment_prefix())
        elif args.subject is not None:
            problems = subject_problems(args.subject)
        else:
            problems = range_problems(args.range)
    except (CannotCheck, OSError) as error:
        print(f"Couldn't check: {error}", file=sys.stderr)
        return 2
    for problem in problems:
        print(f"Commit rule: {problem}", file=sys.stderr)
    if problems:
        print("See the commit rule in CONTRIBUTING.md.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
