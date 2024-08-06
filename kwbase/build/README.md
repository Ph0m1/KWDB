# Docker Deploy

Installing docker is a prerequisite. The instructions differ depending on the
environment. Docker is comprised of two parts: the daemon server which runs on
Linux and accepts commands, and the client which is a Go program capable of
running on MacOS, all Unix variants and Windows.

## Docker Installation

Follow the [Docker install
instructions](https://docs.docker.com/engine/installation/).

## Available images

There are development and deploy images available.

### Development

The development image is a bulky image containing a complete build toolchain.
It is well suited to hacking around and running the tests (including the
acceptance tests). To fetch this image, run `./builder.sh pull`. The image can
be run conveniently via `./builder.sh`.

Note that if you use the builder image, you should ensure that your
Docker installation grants 4GB or more of RAM to containers. On some
systems, the default configuration limits containers to 2GB memory
usage and this can be insufficient to build/link a KaiwuDB
executable.

### Deployment

The deploy image is a downsized image containing a minimal environment for
running KaiwuDB. It is based on Debian Jessie and contains only the main
KaiwuDB binary. To fetch this image, run `docker pull kwbasedb/kwbase` in the usual fashion.

To build the image yourself, use the Dockerfile in the `deploy` directory after
building a release version of the binary with the development image described in
the previous section. The KaiwuDB binary will be built inside of that
development container, then placed into the minimal deployment container. The
resulting image `kwbasedb/kwbase` can be run via `docker run` in the
usual fashion. To be more specific, the steps to do this are:

```
go/src/gitee.com/kwbasedb/kwbase $ ./build/builder.sh mkrelease linux-gnu
go/src/gitee.com/kwbasedb/kwbase $ cp ./kwbase-linux-2.6.32-gnu-amd64 build/deploy/kwbase
go/src/gitee.com/kwbasedb/kwbase $ cd build/deploy && docker build -t kwbasedb/kwbase .
```

The list of valid/recognized targets is available in the script
`build/build/mkrelease.sh`, for example `amd64-linux-gnu` and
`amd64-darwin`. Note that this script supports experimental targets
which may or may not work (and are not officially supported).

# Upgrading / extending the Docker image

Process:

- edit `build/Dockerfile` as desired
- run `build/builder.sh init` to test -- this will build the image locally. Beware this can take a lot of time. The result of `init` is a docker image version which you can subsequently stick into the `version` variable inside the `builder.sh` script for testing locally.
- Once you are happy with the result, run `build/builder.sh push` which pushes your image towards Docker hub, so that it becomes available to others. The result is again a version number, which you then *must* copy back into `builder.sh`. Then commit the change to both Dockerfile and `builder.sh` and submit a PR.
- Finally, use this version number to update the `builder.dockerImage` configuration parameter in TeamCity under the [`kwbase`](https://teamcity.kwbasedb.com/admin/editProject.html?projectId=kwbase&tab=projectParams) and [`Internal`](https://teamcity.kwbasedb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

# Dependencies

A snapshot of KaiwuDB's dependencies is maintained at
https://gitee.com/kwbasedb/vendored and checked out as a submodule at
`./vendor`.

## Updating the golang version

Please copy this checklist into the relevant commit message, with a link
back to this document:

* [ ] Adjust version in Docker image ([source](./builder/Dockerfile#L199-L200)).
* [ ] Rebuild the Docker image and bump the version in builder.sh accordingly ([source](./builder.sh#L6)).
* [ ] Bump the version in go-version-check.sh ([source](./go-version-check.sh)), unless bumping to a new patch release.
* [ ] Bump the default installed version of Go in bootstrap-debian.sh ([source](./bootstrap/bootstrap-debian.sh#L40-42)).
* [ ] Update the `builder.dockerImage` parameter in the TeamCity `kwbase` project.

You can test the new builder image in TeamCity by using the custom parameters
UI (the "..." icon next to the "Run" button) to verify the image before
committing the change.

## Updating the nodejs version

Please follow the instructions above on updating the golang version, omitting the go-version-check.sh step.

## Updating Dependencies

This snapshot was built and is managed using `dep` and we manage `vendor` as a
submodule.

Use the version of `dep` in `bin` (may need to `make` first): import your new
dependency from the Go source you're working on, then run `./bin/dep ensure`.

### Working with Submodules

To keep the bloat of all the changes in all our dependencies out of our main
repository, we embed `vendor` as a git submodule, storing its content and
history in [`vendored`](https://gitee.com/kwbasedb/vendored) instead.

This split across two repositories however means that changes involving
changed dependencies require a two step process.

- After using dep to add or update dependencies and making related code
  changes, `git status` in `kwbasedb/kwbase` checkout will report that the
  `vendor` submodule has `modified/untracked content`.
- Switch into `vendor` and commit all changes (or use `git -C vendor`), on a
  new named branch.
  
  + At this point the `git status` in your `kwbasedb/kwbase` checkout
    will report `new commits` for `vendor` instead of `modified content`.
- Commit your code changes and new `vendor` submodule ref.
- Before this commit can be submitted in a pull request to
  `kwbasedb/kwbase`, the submodule commit it references must be available
  on `gitee.com/kwbasedb/vendored`.

* Organization members can push their named branches there directly.
* Non-members should fork the `vendored` repo and submit a pull request to
  `kwbasedb/vendored`, and need wait for it to merge before they will be able
  to use it in a `kwbasedb/kwbase` PR.

#### `master` Branch Pointer in Vendored Repo

Since the `kwbasedb/kwbase` submodule references individual commit
hashes in `vendored`, there is little significance to the `master` branch in
`vendored` -- as outlined above, new commits are always authored with the
previously referenced commit as their parent, regardless of what `master`
happens to be.

That said, it is critical that any ref in `vendored` that is referenced from
`kwbasedb/kwbase` remain available in `vendored` in perpetuity: after a
PR referencing a ref merges, the `vendored` `master` branch should be updated
to point to it before the named feature branch can be deleted, to ensure the
ref remains reachable and thus is never garbage collected.

#### Conflicting Submodule Changes

The canonical linearization of history is always the main repo. In the event
of concurrent changes to `vendor`, the first should cause the second to see a
conflict on the `vendor` submodule pointer. When resolving that conflict, it
is important to re-run dep against the fetched, updated `vendor` ref, thus
generating a new commit in the submodule that has as its parent the one from
the earlier change.

## Repository Name

We only want the vendor directory used by builds when it is explicitly checked
out *and managed* as a submodule at `./vendor`.

If a go build fails to find a dependency in `./vendor`, it will continue
searching anything named "vendor" in parent directories. Thus the vendor
repository is _not_ named "vendor", to minimize the risk of it ending up
somewhere in `GOPATH` with the name `vendor` (e.g. if it is manually cloned),
where it could end up being unintentionally used by builds and causing
confusion.


