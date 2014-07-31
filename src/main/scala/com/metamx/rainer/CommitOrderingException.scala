package com.metamx.rainer

class CommitOrderingException(val key: Commit.Key, val expectedVersion: Int, val providedVersion: Int)
  extends RuntimeException(
    "Concurrent modification: %s: requested version (%d) was not next available version (%d)".format(
      key, providedVersion, expectedVersion
    )
  )
