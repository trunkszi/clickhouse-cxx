#!/usr/bin/env bash
# Installs the toolchain rust-toolchain.toml names and exports it as RUSTUP_TOOLCHAIN for the
# rest of the job. Extra components are passed as arguments.
#
# Every runner on a machine shares the actions-runner user's ~/.rustup. Updating the `nightly`
# toolchain in place would swap rustc under a job that is still building, and the crates it
# compiles afterwards fail with E0514 against the ones compiled before. A nightly channel is
# therefore installed under its date (`nightly-YYYY-MM-DD`), which no later update touches.
# When a date lacks a requested component, earlier dates are tried, as rustup does for
# `nightly`. Installs hold one lock, so two jobs never write ~/.rustup at once, and dated
# nightlies older than a week are removed under the same lock.
set -euo pipefail

toml=rust-toolchain.toml
channel="$(sed -n 's/^channel = "\(.*\)"/\1/p' "$toml")"
list() { sed -n "s/^$1 = \[\(.*\)\]/\1/p" "$toml" | tr -d '" ' | tr ',' ' '; }
components="$(list components) $*"
targets="$(list targets)"

args=(--profile minimal)
for c in $components; do args+=(--component "$c"); done
for t in $targets; do args+=(--target "$t"); done

rustup_home="${RUSTUP_HOME:-$HOME/.rustup}"
mkdir -p "$rustup_home"
exec 9>"$rustup_home/questra-ci.lock"
flock 9

if [ "$channel" = nightly ]; then
  latest="$(curl -fsS --retry 3 https://static.rust-lang.org/dist/channel-rust-nightly-date.txt)"
  toolchain=""
  for back in 0 1 2 3 4 5 6; do
    candidate="nightly-$(date -u -d "$latest - $back day" +%F)"
    if rustup toolchain install "$candidate" "${args[@]}"; then
      toolchain="$candidate"
      break
    fi
    echo "$candidate lacks a requested component or target; trying the day before"
  done
  if [ -z "$toolchain" ]; then
    echo "::error::no nightly from the last 7 days up to $latest has: $components"
    exit 1
  fi
  cutoff="$(date -u -d "$latest - 7 day" +%F)"
  for old in $(rustup toolchain list | grep -o '^nightly-[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}' || true); do
    if [[ "${old#nightly-}" < "$cutoff" ]]; then
      rustup toolchain uninstall "$old"
    fi
  done
else
  toolchain="$channel"
  rustup toolchain install "$toolchain" "${args[@]}"
fi

if [ -n "${GITHUB_ENV:-}" ]; then
  echo "RUSTUP_TOOLCHAIN=$toolchain" >> "$GITHUB_ENV"
fi
RUSTUP_TOOLCHAIN="$toolchain" rustc --version
RUSTUP_TOOLCHAIN="$toolchain" cargo --version
