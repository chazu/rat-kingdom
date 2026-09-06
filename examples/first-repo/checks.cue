// A README-only teaching repository. Use your repository's real verification
// command when adapting this example to application code.
checks: [
    {
        name: "verify"
        command: "test -s README.md"
        timeout: "30s"
        environmentPolicy: "strip_rk_spawn"
        toolchain: "POSIX shell and Git"
    },
    {
        name: "steward-protected-paths"
        command: "files=$(git diff --name-only \"$RK_CHECK_TARGET\"...HEAD) || exit 1; if printf '%s\\n' \"$files\" | grep -qE \"$RK_CHECK_PROTECTED_PATHS\"; then exit 1; fi"
        timeout: "30s"
    },
    {
        name: "steward-diff-scope"
        command: """
            files=$(git diff --name-only "$RK_CHECK_TARGET"...HEAD) || exit 1
            test "$files" = README.md || exit 1
            stats=$(git diff --numstat "$RK_CHECK_TARGET"...HEAD) || exit 1
            printf '%s\n' "$stats" | awk -v max="$RK_CHECK_MAX_DIFF_LINES" '$1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/ {bad=1} {lines += $1+$2} END {exit (bad || (max != 0 && lines > max))}'
            """
        timeout: "30s"
    },
]
