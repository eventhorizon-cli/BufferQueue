#!/usr/bin/env bash
set -euo pipefail

# Tag events have no branch filter. Check the peeled commit against main explicitly.
git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main
if ! git merge-base --is-ancestor HEAD origin/main; then
  echo "::error title=Release outside main::Release tags must point to a commit on main."
  exit 1
fi

case "$TAG_NAME" in
  core-v*)
    target=core
    version="${TAG_NAME#core-v}"
    ;;
  mmf-v*)
    target=mmf
    version="${TAG_NAME#mmf-v}"
    ;;
  v*)
    target=all
    version="${TAG_NAME#v}"
    ;;
  *)
    echo "::error title=Invalid release tag::Expected core-v<semver>, mmf-v<semver>, or v<semver>."
    exit 1
    ;;
esac

semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?$'
if [[ ! "$version" =~ $semver_pattern ]]; then
  echo "::error title=Invalid release version::Expected a SemVer version without build metadata."
  exit 1
fi

core_version="$version"
if [[ "$target" == mmf ]]; then
  # Reuse the closest core release in this commit's history, including legacy joint releases.
  if ! core_tag=$(git describe --tags --abbrev=0 --match 'core-v[0-9]*' --match 'v[0-9]*' HEAD); then
    echo "::error title=Missing core release::Publish BufferQueue from main before publishing MMF alone."
    exit 1
  fi
  core_version="${core_tag#core-}"
  core_version="${core_version#v}"
  if [[ ! "$core_version" =~ $semver_pattern ]]; then
    echo "::error title=Invalid core release::The nearest core release tag is not valid SemVer."
    exit 1
  fi
  if [[ "$version" != *-* && "$core_version" == *-* ]]; then
    echo "::error title=Prerelease core dependency::A stable MMF release requires a stable core release."
    exit 1
  fi
  if ! git diff --quiet "$core_tag" HEAD -- src/BufferQueue; then
    echo "::error title=Unreleased core changes::Publish BufferQueue first, or use a joint v<semver> tag."
    exit 1
  fi
  # A tag alone is not evidence that its dependency reached the package registry.
  core_path_version=$(printf '%s' "$core_version" | tr '[:upper:]' '[:lower:]')
  curl --fail --silent --show-error --location --retry 3 --output /dev/null \
    "https://api.nuget.org/v3-flatcontainer/bufferqueue/$core_path_version/bufferqueue.$core_path_version.nupkg"
fi

# Per-project versions prevent an MMF-only tag from inventing a same-version core dependency.
# This file exists only in the runner's temporary directory; project files stay version-free.
release_props="$RUNNER_TEMP/BufferQueue.Release.props"
cat > "$release_props" <<EOF_PROPS
<Project>
  <PropertyGroup Condition="'\$(MSBuildProjectName)' == 'BufferQueue'">
    <Version>$core_version</Version>
    <PackageVersion>$core_version</PackageVersion>
  </PropertyGroup>
  <PropertyGroup Condition="'\$(MSBuildProjectName)' == 'BufferQueue.MemoryMappedFile'">
    <Version>$version</Version>
    <PackageVersion>$version</PackageVersion>
  </PropertyGroup>
</Project>
EOF_PROPS

{
  echo "target=$target"
  echo "version=$version"
  echo "core_version=$core_version"
} >> "$GITHUB_OUTPUT"
echo "RELEASE_PROPS=$release_props" >> "$GITHUB_ENV"
