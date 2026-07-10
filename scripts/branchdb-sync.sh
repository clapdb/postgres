#!/usr/bin/env bash
#
# branchdb-sync.sh — 维护 branchdb_* 补丁分支跨 PostgreSQL 大版本的辅助脚本
#
# 模型：每个大版本一条分支 branchdb_<N>，= 上游 REL_<N>_STABLE + 你的一摞补丁。
#       你的补丁永远 rebase 在上游之上，绝不 merge 上游进来。
#
# 用法：
#   scripts/branchdb-sync.sh fetch
#       拉取上游所有 stable 分支与 tag。
#
#   scripts/branchdb-sync.sh minor <N>
#       跟进某大版本的上游小版本更新（如 18.4 -> 18.5）。
#       把 branchdb_<N> 上的补丁 rebase 到最新的 upstream/REL_<N>_STABLE。
#       例: scripts/branchdb-sync.sh minor 18
#
#   scripts/branchdb-sync.sh forward <FROM> <TO>
#       把 branchdb_<FROM> 上的补丁移植到新大版本，生成/更新 branchdb_<TO>。
#       例: scripts/branchdb-sync.sh forward 18 19
#
#   scripts/branchdb-sync.sh status
#       显示每条 branchdb_* 分支领先其上游基线多少个补丁。
#
set -euo pipefail

UPSTREAM=upstream

# 给定大版本号，返回它应对齐的上游 ref。stable 分支出现前回退到最新的
# prerelease tag（RC 优先于 BETA，同类取最大编号）——硬编码 BETA1 会在
# BETA2/RC 窗口期悄悄漏掉后续修复。
upstream_ref() {
  local v=$1
  if git rev-parse --verify --quiet "$UPSTREAM/REL_${v}_STABLE" >/dev/null; then
    echo "$UPSTREAM/REL_${v}_STABLE"
    return 0
  fi
  # BETA* < RC* 恰好也是字母序，sort -V 同时处理编号，tail 即最新
  local tag
  tag=$(git tag -l "REL_${v}_BETA*" "REL_${v}_RC*" | sort -V | tail -1)
  if [[ -n "$tag" ]]; then
    echo "$tag"
  else
    echo "ERROR: 找不到 PG $v 的上游分支或 prerelease tag" >&2
    return 1
  fi
}

cmd_fetch() {
  git fetch "$UPSTREAM" --tags --prune
}

cmd_minor() {
  local v=$1
  local ref; ref=$(upstream_ref "$v")
  local branch="branchdb_$v"
  # 当前补丁所基于的上游提交（即 branch 与上游的 merge-base）
  local oldbase; oldbase=$(git merge-base "$branch" "$ref")
  echo ">> rebase $branch 的补丁: $oldbase -> $ref"
  git rebase --onto "$ref" "$oldbase" "$branch"
  echo ">> 完成。$branch 现已基于 $ref"
}

cmd_forward() {
  local from=$1 to=$2
  local to_ref; to_ref=$(upstream_ref "$to")
  local from_branch="branchdb_$from"
  # from 分支上属于补丁的提交区间（上游基线之后的所有提交）
  local from_base; from_base=$(git merge-base "$from_branch" "$(upstream_ref "$from")")
  echo ">> 把 $from_branch 中 $from_base..$from_branch 的补丁移植到 branchdb_$to (基于 $to_ref)"
  git branch -f "branchdb_$to" "$to_ref"
  git checkout "branchdb_$to"
  git cherry-pick "$from_base..$from_branch"
  echo ">> 完成。如有冲突请解决后 git cherry-pick --continue"
}

cmd_status() {
  for b in $(git for-each-ref --format='%(refname:short)' 'refs/heads/branchdb_*'); do
    v=${b#branchdb_}
    ref=$(upstream_ref "$v" 2>/dev/null) || { printf "%-12s (无上游基线)\n" "$b"; continue; }
    base=$(git merge-base "$b" "$ref")
    n=$(git rev-list --count "$base..$b")
    printf "%-12s 基于 %-28s 领先 %s 个补丁\n" "$b" "$ref" "$n"
  done
}

case "${1:-}" in
  fetch)   cmd_fetch ;;
  minor)   cmd_minor "$2" ;;
  forward) cmd_forward "$2" "$3" ;;
  status)  cmd_status ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
