# Buffered frame regression tests

Two test-only checks exercise existing broker framing behavior. They do not
include the [unapplied classification performance draft](../frame-classification-20261108/README.md).

Source commit `0f4261da256552640db4ecdfa6ef45c840122ad2` has parent main
`e487f7294773f75112f15437992ed780b53c7307` and changes only
`partitionline-broker/src/transport.rs`, appending the tests. Its source blob
is `b84ca76cab8d8340147795e9665584326657cc9c`; the production portion is
unchanged from transport blob `a36cf086c3d683a5f24b818705b15d8ce54245f4`.
Main `a597e6ffe70424f47dd3adcce594f868ac410ce9` has that same production
transport blob and broker manifest blob
`4e23c42f8d9ac722005098a4b58e1046b711e1a3`. Changes since the test's parent
were documentation and top-level client tests; no broker file changed.

The first test starts with a complete first frame and two prefetched bytes of a
second frame. An always-ready reader returns the remaining bytes one at a time;
after the existing read-count yield returns Pending, the test drops the read
future, checks all retained bytes, resumes the read, and verifies both frame
bodies and order. The second test supplies negative and over-limit fragmented
lengths, checks for `InvalidLength`, and checks that no body bytes were read.
The cancellation case specifically uses the existing 128-read voluntary yield;
it does not simulate every socket Pending, TLS, handler cancellation, or
credential lifecycle.

[42475770328](https://github.com/mingley/partitionline/actions/runs/42475770328)
used workflow-only child `def4c9374a741f5c2622c92443e31eb0141c0585`, verified
the exact parent commit and transport blob in each job, and used stable rustc
1.99.1. Both tests passed independently under default features and under all
features. Format check and strict all-target/all-feature Clippy passed.

In the default profile, two independently restored mutants failed at their
intended assertions. Clearing buffered bytes before the voluntary yield failed
the cancellation-retention check. The invalid-prefix control disabled **both**
the extraction and read-loop invalid-length checks; it failed the
invalid-prefix assertion. This is a combined negative control because either
existing check alone rejects the invalid prefix. These controls do not replace
the full broker suite or establish broader production qualification.
