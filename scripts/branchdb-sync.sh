#!/usr/bin/env bash
#
# branchdb-sync.sh -- maintain the branchdb_<N> release branches against
# upstream PostgreSQL and against the pagestore development trunk.
#
# Model (see <repo>/scratchpad or the V4 plan for the full rationale):
#   * pagestore is the development trunk; it tracks the newest upstream
#     stable branch by MERGING it in, never a rebase.
#   * Each branchdb_<N> = an upstream release tag (pinned, not a moving
#     branch head) + a small, linear, curated core patch series (each commit
#     carrying a "Branchdb-Series: C<n>" trailer naming which of the C1-C7
#     logical series it belongs to) + a byte-identical copy of
#     contrib/pagestore taken from a specific pagestore commit.
#   * branchdb_<N> is rebased onto a newer upstream tag when its major gets a
#     minor release ("minor"); it never carries upstream merge commits.
#
# Usage:
#   scripts/branchdb-sync.sh [--dry-run] fetch
#       Fetch upstream's stable branches and tags, and origin.
#
#   scripts/branchdb-sync.sh [--dry-run] status
#       For every origin/branchdb_* branch: the upstream tag it sits on (or
#       "not pinned to a tag" when its merge-base with upstream/REL_<N>_STABLE
#       is not itself tagged), how many commits and how many merge commits it
#       carries above that base, and whether contrib/pagestore matches the
#       contrib_sha recorded for that major in release-branches.json.
#
#   scripts/branchdb-sync.sh [--dry-run] minor <N> [TAG]
#       Rebase branchdb_<N>'s patch series onto a newer upstream tag (default:
#       the newest non-prerelease REL_<N>_* tag). Refuses if branchdb_<N>
#       carries any merge commit above its current base -- a merge there means
#       the branch is not in the "tag + linear series" shape minor expects.
#       Example: scripts/branchdb-sync.sh minor 18 REL_18_7
#
#   scripts/branchdb-sync.sh [--dry-run] forward <FROM> <TO> [TAG]
#       Build (or refresh) branchdb_<TO>-rc from an upstream tag (default: the
#       newest non-prerelease REL_<TO>_* tag) by cherry-picking, oldest first,
#       every commit in FROM's range above its own upstream base whose commit
#       message carries a "Branchdb-Series: C[1-7]" trailer. FROM may be a
#       branchdb_<N> branch or "pagestore" itself. Refuses (does nothing) when
#       no commit in range carries the trailer, rather than silently cherry-
#       picking nothing or falling back to the raw commit range -- today's
#       pagestore commits do not carry the trailer yet; a later PR adds it
#       when the core series is squashed into C1-C7 (see the V4 plan, P2).
#       Example: scripts/branchdb-sync.sh forward pagestore branchdb_18
#
#   scripts/branchdb-sync.sh [--dry-run] sync-contrib <branch> <SHA>
#       Replace <branch>'s contrib/pagestore with the tree at <SHA> (a commit
#       on pagestore) and commit, recording <SHA> in the commit message.
#       Refuses if the result is not byte-identical to <SHA>'s contrib tree.
#
#   scripts/branchdb-sync.sh [--dry-run] verify <branch> <SHA>
#       The checks a release-branch PR must pass: the branch's base is an
#       upstream tag (not a moving branch head), zero merge commits above
#       that base, contrib/pagestore is byte-identical to <SHA>, and every
#       non-contrib commit above the base either carries a
#       "Branchdb-Series:" trailer or has a ci:/docs:/fixtures: subject
#       prefix. Add --require-build-match and a build directory
#       (--build DIR) to additionally require that DIR's compiled fixture
#       identities match a fixture recorded for <branch>'s major (a thin
#       wrapper around pagestore_fixture.py --check --require-build-match;
#       skipped unless --build is given).
#
# --dry-run (any command, before the subcommand or after) prints the git
# commands a mutating subcommand would run instead of running them; fetch,
# status and verify are read-only and always run (the flag is accepted but
# has nothing to skip).
#
# branchdb-sync.sh never touches "pagestore" or "master": every subcommand
# that takes a branch/target argument refuses outright if it is either name.
#
set -euo pipefail

SELF="$(basename "$0")"
UPSTREAM=upstream
DRY_RUN=0
RELEASE_BRANCHES_JSON="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/contrib/pagestore/release-branches.json"

usage() {
  # Print everything from the header comment block (between the shebang and
  # the "set -euo pipefail" line) with the leading "# " stripped.
  awk '/^#!/{next} /^set -euo pipefail/{exit} {sub(/^# ?/, ""); print}' "$0"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# run CMD...  -- executes CMD unless DRY_RUN=1, in which case it only prints
# what would run.  Use for every git command with a side effect.
run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+ (dry-run, not run) '
    printf '%q ' "$@"
    printf '\n'
    return 0
  fi
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  "$@"
}

refuse_protected_branch() {
  local branch=$1
  case "$branch" in
    pagestore|master)
      die "$SELF refuses to touch '$branch' directly; operate on a branchdb_* or a staging branch instead"
      ;;
  esac
}

major_of_branch() {
  local branch=$1
  if [[ "$branch" =~ ^branchdb_([0-9]+)(-rc)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

# upstream_ref N -- the upstream ref branchdb_N should sit on: the stable
# branch once it exists, otherwise the newest prerelease tag (RC over BETA,
# highest number of either) so a BETA1-pinned branch does not silently miss
# later prerelease fixes while nobody has run `minor` yet.
upstream_ref() {
  local v=$1
  if git rev-parse --verify --quiet "$UPSTREAM/REL_${v}_STABLE" >/dev/null; then
    echo "$UPSTREAM/REL_${v}_STABLE"
    return 0
  fi
  local tag
  tag=$(git tag -l "REL_${v}_BETA*" "REL_${v}_RC*" | sort -V | tail -1)
  if [[ -n "$tag" ]]; then
    echo "$tag"
  else
    echo "ERROR: no upstream stable branch or prerelease tag found for PG $v" >&2
    return 1
  fi
}

# newest_release_tag N -- the newest non-prerelease REL_N_* tag, or empty if
# none exists yet (e.g. PG 19 before its first GA/RC tag).
newest_release_tag() {
  local v=$1
  git tag -l "REL_${v}_*" | { grep -vE 'BETA|RC' || true; } | sort -V | tail -1
}

# ---- fetch -----------------------------------------------------------------

cmd_fetch() {
  run git fetch "$UPSTREAM" --tags --prune
  run git fetch origin --prune
}

# ---- status ------------------------------------------------------------------

release_branches_field() {
  # release_branches_field MAJOR FIELD -- best-effort JSON field lookup
  # without a JSON library dependency (stdlib git only); tolerant of the
  # field being null/absent.
  local major=$1 field=$2
  python3 - "$RELEASE_BRANCHES_JSON" "$major" "$field" <<'PY' 2>/dev/null || true
import json, sys
path, major, field = sys.argv[1], int(sys.argv[2]), sys.argv[3]
try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
except (OSError, ValueError):
    sys.exit(0)
for entry in data.get("supported", []) + data.get("unsupported", []):
    if entry.get("major") == major:
        value = entry.get(field)
        if value is not None:
            print(value)
        break
PY
}

cmd_status() {
  local any=0
  for ref in $(git for-each-ref --format='%(refname:short)' 'refs/remotes/origin/branchdb_*'); do
    any=1
    local branch=${ref#origin/}
    local v
    v=$(major_of_branch "$branch") || { printf '%-14s (not a branchdb_<N> branch)\n' "$branch"; continue; }
    local upref
    if ! upref=$(upstream_ref "$v" 2>/dev/null); then
      printf '%-14s no upstream stable branch or tag for PG %s\n' "$branch" "$v"
      continue
    fi
    local base
    base=$(git merge-base "$ref" "$upref")
    local base_tag
    if base_tag=$(git describe --tags --exact-match "$base" 2>/dev/null); then
      base_desc="$base_tag"
    else
      base_desc="$(git describe --tags --abbrev=0 "$base" 2>/dev/null || echo "$base")+ (not pinned to a tag)"
    fi
    local commits merges
    commits=$(git rev-list --count "$base..$ref")
    merges=$(git rev-list --count --merges "$base..$ref")
    local contrib_sha contrib_state
    contrib_sha=$(release_branches_field "$v" contrib_sha)
    if [[ -z "$contrib_sha" ]]; then
      contrib_state="no contrib_sha recorded in release-branches.json"
    elif git diff --quiet "$contrib_sha" "$ref" -- contrib/pagestore 2>/dev/null; then
      contrib_state="contrib/pagestore == pagestore@${contrib_sha:0:12}"
    else
      contrib_state="contrib/pagestore DIFFERS from pagestore@${contrib_sha:0:12}"
    fi
    printf '%-14s base %-40s %3d commits (%d merges)   %s\n' \
      "$branch" "$base_desc" "$commits" "$merges" "$contrib_state"
  done
  if [[ "$any" == "0" ]]; then
    echo "no origin/branchdb_* branches found (run '$SELF fetch' first?)" >&2
    return 1
  fi
}

# ---- minor -------------------------------------------------------------------

cmd_minor() {
  local v=${1:-} tag=${2:-}
  [[ -n "$v" ]] || die "usage: $SELF minor <N> [TAG]"
  local branch="branchdb_$v"
  refuse_protected_branch "$branch"
  git rev-parse --verify --quiet "$branch" >/dev/null || die "no local branch '$branch' (checkout: git checkout -b $branch origin/$branch)"
  if [[ -z "$tag" ]]; then
    tag=$(newest_release_tag "$v")
    [[ -n "$tag" ]] || die "no non-prerelease REL_${v}_* tag exists yet; pass TAG explicitly"
  fi
  git rev-parse --verify --quiet "$tag" >/dev/null || die "unknown ref '$tag' (run '$SELF fetch'?)"
  local upref
  upref=$(upstream_ref "$v") || die "no upstream ref for PG $v"
  local oldbase
  oldbase=$(git merge-base "$branch" "$upref")
  local merges
  merges=$(git rev-list --count --merges "$oldbase..$branch")
  if [[ "$merges" != "0" ]]; then
    die "$branch carries $merges merge commit(s) above $oldbase; minor only rebases a linear series (see 'verify')"
  fi
  echo ">> rebasing $branch's patch series: $oldbase -> $tag"
  run git rebase --onto "$tag" "$oldbase" "$branch"
  echo ">> done; $branch is now based on $tag"
}

# ---- forward -----------------------------------------------------------------

cmd_forward() {
  local from=${1:-} to=${2:-} tag=${3:-}
  [[ -n "$from" && -n "$to" ]] || die "usage: $SELF forward <FROM> <TO> [TAG]"
  # TO names the target major: a bare number (18), a branch (branchdb_18) or
  # a staging branch (branchdb_18-rc) are all accepted.
  local to_major
  if [[ "$to" =~ ^[0-9]+$ ]]; then
    to_major="$to"
  elif to_major=$(major_of_branch "$to"); then
    :
  else
    die "TO must be a PG major number or a branchdb_<N>[-rc] branch, not '$to'"
  fi
  local to_branch="branchdb_$to_major"
  refuse_protected_branch "$to_branch"
  local rc_branch="${to_branch}-rc"

  if [[ "$from" =~ ^[0-9]+$ ]]; then
    from="branchdb_$from"
  fi

  local from_base
  if [[ "$from" == "pagestore" || "$from" == "origin/pagestore" ]]; then
    git rev-parse --verify --quiet origin/pagestore >/dev/null || die "no origin/pagestore (run '$SELF fetch'?)"
    local from_upref
    # pagestore tracks the newest upstream stable; that is also the ref its
    # core-series commits are measured against.
    from_upref=$(upstream_ref 19) || die "no upstream ref to measure pagestore's core series against"
    from_base=$(git merge-base origin/pagestore "$from_upref")
    from=origin/pagestore
  else
    local from_v
    from_v=$(major_of_branch "$from") || die "FROM must be 'pagestore' or a branchdb_<N>[-rc] branch, not '$from'"
    git rev-parse --verify --quiet "$from" >/dev/null 2>&1 || from="origin/$from"
    git rev-parse --verify --quiet "$from" >/dev/null || die "no ref '$from' or 'origin/$from'"
    local from_upref
    from_upref=$(upstream_ref "$from_v") || die "no upstream ref for PG $from_v"
    from_base=$(git merge-base "$from" "$from_upref")
  fi

  if [[ -z "$tag" ]]; then
    tag=$(newest_release_tag "$to_major")
    [[ -n "$tag" ]] || die "no non-prerelease REL_${to_major}_* tag exists yet; pass TAG explicitly"
  fi
  git rev-parse --verify --quiet "$tag" >/dev/null || die "unknown ref '$tag' (run '$SELF fetch'?)"

  local -a series_commits=()
  while IFS= read -r sha; do
    [[ -n "$sha" ]] && series_commits+=("$sha")
  done < <(git log --reverse --format=%H --grep='^Branchdb-Series:' -E "$from_base..$from")

  if [[ ${#series_commits[@]} -eq 0 ]]; then
    cat >&2 <<EOF
ERROR: no commit in $from_base..$from carries a "Branchdb-Series: C<n>" trailer;
refusing to build $rc_branch from an unmarked range (that would either
cherry-pick nothing or, worse, the raw unmarked history). The C1-C7 core
series gets its trailers when it is squashed onto the target branch (V4
plan, P2); run 'forward' again after that lands.
EOF
    return 1
  fi

  echo ">> ${#series_commits[@]} core-series commit(s) found in $from_base..$from:"
  for sha in "${series_commits[@]}"; do
    local trailer subject
    subject=$(git log -1 --format=%s "$sha")
    trailer=$(git show -s --format=%B "$sha" | grep -m1 '^Branchdb-Series:' || echo "Branchdb-Series: ?")
    printf '   %s  %-10s %s\n' "${sha:0:12}" "$trailer" "$subject"
  done

  echo ">> building $rc_branch at $tag and cherry-picking the series above"
  run git branch -f "$rc_branch" "$tag"
  run git checkout "$rc_branch"
  run git cherry-pick -x "${series_commits[@]}"
  echo ">> done (or, on conflict, resolve and 'git cherry-pick --continue'). Next: '$SELF sync-contrib $rc_branch <pagestore SHA>'"
}

# ---- sync-contrib --------------------------------------------------------------

cmd_sync_contrib() {
  local branch=${1:-} sha=${2:-}
  [[ -n "$branch" && -n "$sha" ]] || die "usage: $SELF sync-contrib <branch> <SHA>"
  refuse_protected_branch "$branch"
  git rev-parse --verify --quiet "$sha" >/dev/null || die "unknown commit '$sha'"
  local current
  current=$(git rev-parse --abbrev-ref HEAD)
  if [[ "$current" != "$branch" ]]; then
    run git checkout "$branch"
  fi
  run git rm -rq --ignore-unmatch contrib/pagestore
  run git checkout "$sha" -- contrib/pagestore
  run git add contrib/pagestore
  run git commit -m "contrib/pagestore: sync to pagestore@${sha}"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo ">> (dry-run) would verify contrib/pagestore is now byte-identical to ${sha}"
    return 0
  fi
  if ! git diff --quiet "$sha" HEAD -- contrib/pagestore; then
    die "contrib/pagestore on $branch is not byte-identical to ${sha} after sync-contrib (this should not happen)"
  fi
  echo ">> contrib/pagestore on $branch is byte-identical to pagestore@${sha}"
}

# ---- verify --------------------------------------------------------------------

cmd_verify() {
  local branch=${1:-} sha=${2:-}
  shift $(( $# < 2 ? $# : 2 )) || true
  local require_build_match=0 build=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --require-build-match) require_build_match=1; shift ;;
      --build) build=${2:-}; shift 2 ;;
      *) die "verify: unknown option '$1'" ;;
    esac
  done
  [[ -n "$branch" && -n "$sha" ]] || die "usage: $SELF verify <branch> <SHA> [--build DIR] [--require-build-match]"
  refuse_protected_branch "$branch"
  local v
  v=$(major_of_branch "$branch") || die "verify expects a branchdb_<N>[-rc] branch, not '$branch'"
  local ref="$branch"
  git rev-parse --verify --quiet "$ref" >/dev/null 2>&1 || ref="origin/$branch"
  git rev-parse --verify --quiet "$ref" >/dev/null || die "no ref '$branch' or 'origin/$branch'"
  git rev-parse --verify --quiet "$sha" >/dev/null || die "unknown commit '$sha'"

  local upref
  upref=$(upstream_ref "$v") || die "no upstream ref for PG $v"
  local base
  base=$(git merge-base "$ref" "$upref")

  local failures=0

  if git describe --tags --exact-match "$base" >/dev/null 2>&1; then
    echo "ok   - base $(git describe --tags --exact-match "$base") is an upstream tag"
  else
    echo "FAIL - base ${base:0:12} is not pinned to an upstream tag (merge-base with $upref)"
    failures=$((failures + 1))
  fi

  local merges
  merges=$(git rev-list --count --merges "$base..$ref")
  if [[ "$merges" == "0" ]]; then
    echo "ok   - no merge commits above the base"
  else
    echo "FAIL - $merges merge commit(s) above the base"
    failures=$((failures + 1))
  fi

  if git diff --quiet "$sha" "$ref" -- contrib/pagestore; then
    echo "ok   - contrib/pagestore is byte-identical to pagestore@${sha:0:12}"
  else
    echo "FAIL - contrib/pagestore differs from pagestore@${sha:0:12}"
    failures=$((failures + 1))
  fi

  local untrailered=0
  while IFS= read -r commit_sha; do
    [[ -n "$commit_sha" ]] || continue
    # a commit that touches only contrib/pagestore is a sync-contrib commit;
    # it is exempt (its provenance is the SHA in its own message, checked
    # above via the tree comparison, not via a trailer).
    if [[ -z "$(git diff-tree --no-commit-id --name-only -r "$commit_sha" -- . ':!contrib/pagestore')" ]]; then
      continue
    fi
    local subject
    subject=$(git log -1 --format=%s "$commit_sha")
    if git show -s --format=%B "$commit_sha" | grep -qE '^Branchdb-Series: C[1-7]$'; then
      continue
    fi
    case "$subject" in
      ci:*|docs:*|fixtures:*) continue ;;
    esac
    echo "FAIL - ${commit_sha:0:12} '$subject' has no Branchdb-Series trailer and no ci:/docs:/fixtures: prefix"
    untrailered=$((untrailered + 1))
  done < <(git rev-list --no-merges "$base..$ref")
  if [[ "$untrailered" == "0" ]]; then
    echo "ok   - every non-contrib commit above the base is a Branchdb-Series or ci:/docs:/fixtures: commit"
  else
    failures=$((failures + untrailered))
  fi

  if [[ -n "$build" ]]; then
    local fixture_check="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/contrib/pagestore/harness/pagestore_fixture.py"
    local -a fixtures=()
    while IFS= read -r -d '' dir; do
      fixtures+=("$dir")
    done < <(find "$(dirname "$fixture_check")/../fixtures" -mindepth 1 -maxdepth 1 -type d -name 'posix-*' -print0 2>/dev/null)
    if [[ ${#fixtures[@]} -eq 0 ]]; then
      echo "FAIL - --build given but no contrib/pagestore/fixtures/posix-* directories found"
      failures=$((failures + 1))
    else
      local -a check_args=(
        python3 "$fixture_check" --check "${fixtures[@]}"
        --daemon-binary "$build/contrib/pagestore/pagestore_daemon"
        --client-binary "$build/contrib/pagestore/pagestore_gc_crash_client"
        --inspect-binary "$build/contrib/pagestore/pagestore_inspect"
        --format-tool "$build/contrib/pagestore/pagestore_format_versions"
        --postgres-payload-identity-tool "$build/contrib/pagestore/pagestore_control_restore"
      )
      [[ "$require_build_match" == "1" ]] && check_args+=(--require-build-match)
      # verify is read-only and always executes its checks for real, even
      # under --dry-run (there is nothing to "not do" about reading fixture
      # state); print the command for transparency but do not route it
      # through run(), which would swallow it under --dry-run.
      printf '+ '
      printf '%q ' "${check_args[@]}"
      printf '\n'
      if "${check_args[@]}"; then
        echo "ok   - build-match fixture check passed"
      else
        echo "FAIL - build-match fixture check failed"
        failures=$((failures + 1))
      fi
    fi
  elif [[ "$require_build_match" == "1" ]]; then
    echo "FAIL - --require-build-match given without --build DIR"
    failures=$((failures + 1))
  fi

  if [[ "$failures" == "0" ]]; then
    echo ">> verify $branch $sha: PASS"
    return 0
  else
    echo ">> verify $branch $sha: FAIL ($failures check(s) failed)"
    return 1
  fi
}

# ---- main ------------------------------------------------------------------

main() {
  local args=()
  for a in "$@"; do
    if [[ "$a" == "--dry-run" ]]; then
      DRY_RUN=1
    else
      args+=("$a")
    fi
  done
  set -- "${args[@]+"${args[@]}"}"

  local cmd=${1:-}
  [[ $# -gt 0 ]] && shift || true

  case "$cmd" in
    fetch)         cmd_fetch "$@" ;;
    status)        cmd_status "$@" ;;
    minor)         cmd_minor "$@" ;;
    forward)       cmd_forward "$@" ;;
    sync-contrib)  cmd_sync_contrib "$@" ;;
    verify)        cmd_verify "$@" ;;
    -h|--help|help|"") usage; [[ -z "$cmd" ]] && exit 1 || exit 0 ;;
    *) echo "ERROR: unknown command '$cmd'" >&2; usage; exit 1 ;;
  esac
}

main "$@"
