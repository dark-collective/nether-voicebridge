#!/bin/sh
# Linker wrapper for the fully static build (see Dockerfile `static-builder`).
#
# webrtc-sys declares its system libs (m, dl, pthread, rt) as dylibs, so rustc
# brackets them with -Bdynamic even under +crt-static. GNU ld then refuses to
# put libm.so into a static link, and rust-lld silently accepts it, producing
# a binary that dies in __libc_start_main. Rewriting every -Bdynamic to
# -Bstatic makes each library resolve to its .a archive.
for a in "$@"; do
  shift
  set -- "$@" "$(printf '%s' "$a" | sed 's/-Bdynamic/-Bstatic/g')"
done
exec cc "$@"
