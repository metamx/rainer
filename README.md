## Rainer

Rainer helps you deal with namespaces of versioned key/value pairs called "commits". It includes a variety of APIs
that can collectively provide a full audit trail, concurrency-safe updates, local caching, immediate cluster
notifications, and easy client integration. The Rainer APIs can be used independently, or they can be used together as
part of a powerful configuration management system.

## Commits

Commits have the attributes:

- Key, which is a String.
- Payload, which can be an arbitrary type. Think of this like a short document.
- Version, which must increase by one with each commit.
- Author, the entity that created the commit.
- Comment, some free-form string describing the commit.
- Mtime, the timestamp when the commit was created.

Each key's commits are separate from every other key's commits, and are versioned independently. In particular,
there is no concept of a global namespace version.

Your value type must be deserializable, so you need to provide a KeyValueDeserialization for it. You do not need to
provide a serializer, because your values will never be serialized by Rainer. Commits are always built from raw bytes
and those bytes are stored verbatim. If the documents will be edited by humans, this makes it easy to use formats
with comments, any whitespace you like, and so on.

## Long-term journaled storage

The CommitStorage trait represents a key/value store that retains every version of every commit. Its
default implementation is DbCommitStorage, which is backed by an RDBMS.

```scala
implicit val deserialization = KeyValueDeserialization.usingJackson[ValueType](objectMapper)

val db = new MySQLDB(dbConfig) with DbCommitStorageMySQLMixin
val storage = new DbCommitStorage[ValueType](db, "table_name")
storage.start()

// Revert "foo" to previous version
val foo = storage.get("foo")
val previousFoo = foo flatMap (x => storage.get(x.key, x.version - 1))
previousFoo foreach (x => storage.save(Commit.fromBytes(x.key, x.version + 2, x.payload, "me", "Reverting foo", DateTime.now))
```

## ZooKeeper storage

The CommitKeeper class represents the most recent versions of each commit stored in ZooKeeper. It supports
on-demand access, live-updating views, and instant notifications.

```scala
implicit val deserialization = KeyValueDeserialization.usingJackson[ValueType](objectMapper)

val keeper = new CommitKeeper[ValueType](curator, "/path/in/zk")

// On-demand access (synchronous ZK operations):
val foo = keeper.get("foo")
val keys = keeper.keys

// Create a mirror (async updates from ZK):
val mirror: Var[Map[String, Commit[ValueType]]] = keeper.mirror()

// Which, for example, you can use to update an AtomicReference:
val ref = new AtomicReference[Map[String, Commit[ValueType]]]
val c = mirror.changes.register(Witness(ref))

// When no longer interested in updates:
Await.result(c.close())
```

### Using CommitKeeper with CommitStorage

Combining a CommitStorage with a CommitKeeper is a common pattern used for managing configuration of a cluster of
machines. This setup can provide full-history journaled updates in an RDBMS, coupled with low latency notifications
through ZooKeeper. The idea is to do all of your saves through a keeperPublishing CommitStorage (which will update
ZooKeeper any time you save something) and to do your reads through a mirror provided by a CommitKeeper. To prevent
inopportune crashes or out of band ZooKeeper modifications from causing ZooKeeper to become out of sync, you can also
use an autoPublisher to periodically push any unpushed updates from your storage. This can all be done with something
like:

```scala
// During setup, link your underlying storage and keeper together.
val keeper = new CommitKeeper[ValueType](curator, "/path/in/zk")
val storage = CommitStorage.keeperPublishing(underlyingStorage, keeper)
val autoPublisher = keeper.autoPublisher(storage, period, periodFuzz)
autoPublisher.start()

// You can get a mirror from your keeper, which will now reflect the most recent commits from your storage.
val mirror: Var[Map[String, Commit[ValueType]]] = keeper.mirror()

// When you save to the storage, ZooKeeper will be updated automatically.
// Don't save using the keeper!
storage.save(someCommit)

// When your application exits, shut down your autoPublisher.
Await.result(autoPublisher.close())
```

### Using CommitKeeper without CommitStorage

You can use CommitKeeper by itself, too, without any backing journaled storage. Just call ```keeper.save(commit)``` to
publish new commits, and don't use the autoPublisher. Everything else will work normally, including gets and mirrors.

## HTTP server

You can add a RainerServlet to your Jetty-based server to gain a nice HTTP API to your CommitStorage. The API can
be used like this with any HTTP client:

```
$ curl \
  -s \
  -XPOST \
  -H'Content-Type: application/octet-stream' \
  -H'X-Rainer-Author: gian' \
  -H'X-Rainer-Comment: rofl' \
  --data-binary '{"hey":"what"}' \
  http://localhost:8080/diary/foo/1
{"author":"gian","comment":"rofl","key":"foo","mtime":"2014-02-11T14:01:28.839-08:00","version":1}

$ curl -s http://localhost:8080/diary/foo
{"hey":"what"}

$ curl -s http://localhost:8080/diary/foo/1
{"hey":"what"}

$ curl -s http://localhost:8080/diary/foo/1/meta
{"author":"gian","key":"foo","version":1,"mtime":"2014-02-11T14:01:28.839-08:00","comment":"rofl"}
```

## JVM HTTP client

From another JVM, you can also use the HttpCommitStorage:

```scala
val uri = new URI("http://localhost:8080/diary")
val client = ClientBuilder()
  .name(uri.toString)
  .codec(Http())
  .group(Group.fromVarAddr(InetResolver.bind(uri.getAuthority)))
  .hostConnectionLimit(2)
  .build()
val storage = new HttpCommitStorage[ValueType](client, uri)

// Use it like any other CommitStorage:
val commit = storage.get("hey")
```

## Python HTTP client

From Python, "pyrainer" is a convenient wrapper around the HTTP API. You can install it with ```pip install pyrainer```
and use it like this:

```python
import pyrainer.http

client = pyrainer.http.RainerClient("http://localhost:8080/diary")

commits = client.list()
for key in sorted(commits.keys()):
  version = data[key]['version']
  print "%s [current version = %s]" % (key, str(version))

hey = client.get_commit("hey")
print "Commit version %s = %s" % (hey.meta["version"], hey.value)

client.post_commit({"key": "hey", "version": 2, "author": "gian", "comment": "rofl"}, "new value")
```

## Command-line client

If you are running a RainerServlet, you can use the "rainer" command line tool interactively or in shell scripts.

```
$ bin/rainer --url http://localhost:8080/diary list
foo     1       http://localhost:8080/diary/foo/1
hey     6       http://localhost:8080/diary/hey/6

$ bin/rainer --url http://localhost:8080/diary show hey
{"hey":"whatsit"}

$ rainer --url http://localhost:8080/diary edit hey
[...a wild $EDITOR appears!]
```
