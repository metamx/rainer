# <img width="175" alt="Rainer" src="https://cloud.githubusercontent.com/assets/1214075/3766893/c5322fb2-18c8-11e4-8a92-08a8e45bc5ca.jpg" />

Rainer is a configuration management library that is based around versioned key/value pairs called "commits". We
created it after we noticed that we had a lot of services that each managed configuration in slightly different ways
(with slightly different flaws!). It is composed of a set of APIs can be used independently, or together as part of a
powerful configuration management system. In addition to key/value access, it can also provide:

- Full audit trail of historical commits for each key, including who, why, and when they were committed.
- Ability to detect concurrent modifications and prevent users from clobbering each other.
- Extensibility to handle a variety of storage backends and commit payload types.
- Optional ZooKeeper-backed views and notifications, for immediate cluster-wide updates.
- Optional HTTP API along with clients for the JVM, Python, and the command line.

## Commits

Rainer is based around versioned key/value pairs called "commits". Commits have the attributes:

- Key, which is a String.
- Payload, which can be an arbitrary type. Think of this like a short document.
- Version, which must increase by one with each successive commit.
- Author, the entity that created the commit.
- Comment, some free-form string describing the commit.
- Mtime, the timestamp when the commit was created.

Each key's commits are separate from every other key's commits, and are versioned independently. In particular,
there is no concept of a global database version.

Your payload type must be deserializable, so you need to provide a KeyValueDeserialization for it. You can optionally
provide a KeyValueSerialization as well.

If you are not using a serializer, then when you create a new Commit, instead of providing the payload as an object,
you just provide some bytes that are deserializable using your KeyValueDeserialization. This mode is useful if you want
to allow humans to edit the serialized documents directly, since it will preserve comments, whitespace, and so on.

## Usage

Rainer offers three server-side components that can work together or separately:

- CommitStorage (persistent journaled storage)
- CommitKeeper (ZooKeeper-backed views and notifications)
- RainerServlet (HTTP API for remote inspection and modification)

If you want to use all three together, the simplest way is using the "Rainers" builder, which creates all three and
links them together:

```scala
implicit val serialization = KeyValueSerialization.usingJackson[ValueType](objectMapper)
implicit val deserialization = KeyValueDeserialization.usingJackson[ValueType](objectMapper)

val db = new MySQLDB(dbConfig) with DbCommitStorageMySQLMixin
val rainers = Rainers.create[ValueType](
  curator,
  "/path/in/zk",
  new DbCommitStorage[ValueType](db, "table_name")
)

db.start()
rainers.start()

// Do what you will with these:
val myStorage: CommitStorage[ValueType] = rainers.storage
val myKeeper: CommitKeeper[ValueType] = rainers.keeper
val myServlet: RainerServlet[ValueType] = rainers.servlet

// For example:
myStorage.save(Commit.fromValue(
  key = "foo",
  version = 1,
  payload = new ValueType,
  author = "me",
  comment = "Creating foo",
  mtime = DateTime.now
))
```

## Components

### CommitStorage: Persistent journaled storage

The CommitStorage trait represents a key/value store that retains every version of every commit. The main builtin
implementation is DbCommitStorage, which is backed by an RDBMS.

```scala
implicit val deserialization = KeyValueDeserialization.usingJackson[ValueType](objectMapper)

val db = new MySQLDB(dbConfig) with DbCommitStorageMySQLMixin
val storage = new DbCommitStorage[ValueType](db, "table_name")

db.start()
storage.start()

// Revert "foo" to previous version
val foo = storage.get("foo")
val previousFoo = foo flatMap (x => storage.get(x.key, x.version - 1))
previousFoo foreach (x => storage.save(Commit.fromBytes(x.key, x.version + 2, x.payload, "me", "Reverting foo", DateTime.now))
```

### CommitKeeper: ZooKeeper views

The CommitKeeper class represents the most recent versions of each commit stored in ZooKeeper. It supports
on-demand access, live-updating views, and instant notifications. It does not keep a full history for each key; only
CommitStorages do that.

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

// When you're no longer interested in updates, close the mirror:
Await.result(c.close())
```

### Using CommitKeeper with CommitStorage

Combining a CommitStorage with a CommitKeeper is a common pattern used for managing configuration of a cluster of
machines.  Combined setups can provide full-history journaled updates in an RDBMS, coupled with low latency
notifications through ZooKeeper. The idea is to do all of your saves through a keeperPublishing CommitStorage (which
will update ZooKeeper any time you save something) and to do your reads through a mirror provided by a CommitKeeper. To
prevent inopportune crashes or out of band ZooKeeper modifications from causing ZooKeeper to become out of sync, an
autoPublisher can periodically detect and push any unpushed updates from your storage.

The "Rainers" builder sets all of this up for you, but you can also do it manually with something like:

```scala
// During setup, link your underlying storage and keeper together.
// Afterwards, use "storage", not "underlyingStorage" for your own operations.
val keeper = new CommitKeeper[ValueType](curator, "/path/in/zk")
val storage = CommitStorage.keeperPublishing(underlyingStorage, keeper)
val autoPublisher = keeper.autoPublisher(underlyingStorage, period, periodFuzz)
autoPublisher.start()

// When your application exits, shut down your autoPublisher.
Await.result(autoPublisher.close())
```

When using a linked CommitStorage and CommitKeeper, make sure to send writes through the CommitStorage only. This is
because the CommitStorage is treated as the system of record.

```scala
// You can get a mirror from your keeper, which will now reflect the most recent commits from your storage.
val mirror: Var[Map[String, Commit[ValueType]]] = keeper.mirror()

// Which, for example, you can use to update an AtomicReference:
val ref = new AtomicReference[Map[String, Commit[ValueType]]]
val c = mirror.changes.register(Witness(ref))

// When you save to the storage, ZooKeeper and the mirror will be updated automatically. (Don't save using the keeper!)
storage.save(someCommit)
```

### Using CommitStorage without CommitKeeper

You can use a CommitStorage by itself. The only thing you lose are the ZooKeeper views, which means you can't set up
mirrors. In this case you'll either need to access the underlying storage on-demand, or set up a cache yourself that
refreshes periodically.

### Using CommitKeeper without CommitStorage

You can use CommitKeeper by itself, too, without any backing journaled storage. You won't be able to access old
versions of any commits. Just call ```keeper.save(commit)``` to publish new commits, and don't use the autoPublisher.
The other CommitKeeper features will work normally, including gets and mirrors.

### HTTP server

You can use the RainerServlet to gain a nice HTTP API to your CommitStorage. It does not use, or require, a
CommitKeeper. The API can be used like this with any HTTP client:

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

## Clients

If you have a RainerServlet running, there are a few pre-built clients that can be used to inspect and modify
configurations remotely.

### JVM HTTP client

You can use HttpCommitStorage to access a RainerServlet running on another machine:

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

### Python HTTP client

From Python, you can use "pyrainer". You can install it with ```pip install pyrainer``` and use it like this:

```python
import pyrainer.http

client = pyrainer.http.RainerClient("http://localhost:8080/diary")

commits = client.list_full()
for key in sorted(commits.keys()):
  version = commits[key].version
  print "%s [current version = %s]" % (key, str(version))

hey = client.get_commit("hey")
print "Commit version %s = %s" % (hey.meta["version"], hey.value)

client.post_commit({"key": "hey", "version": 2, "author": "sue", "comment": "rofl"}, "new value")
```

### Command-line client

If you have pyrainer installed (```pip install pyrainer```), you can use the command line tool interactively or in
shell scripts.

```
$ python -m pyrainer.rainer --url http://localhost:8080/diary list
foo     1       http://localhost:8080/diary/foo/1
hey     6       http://localhost:8080/diary/hey/6

$ python -m pyrainer.rainer --url http://localhost:8080/diary show hey
{"hey":"whatsit"}

$ python -m pyrainer.rainer --url http://localhost:8080/diary edit hey
[...a wild $EDITOR appears!]
```

You can also create a wrapper for your service that makes invocation simpler. An executable script like this should
do it:

```bash
#!/bin/sh -e
python -m pyrainer.rainer --url "http://example.com/foo" "$@"
```
