# Apache Arrow Java Memory Allocator System -- Comprehensive Report

## 1. Arrow Java Memory Architecture

### 1.1 BufferAllocator Interface

`BufferAllocator` is the primary public interface for all memory allocation in Arrow Java. It provides:

**Key methods:**
- `buffer(long size)` -- allocate a new `ArrowBuf` of the given size (direct/off-heap memory)
- `newChildAllocator(String name, long initialReservation, long maxAllocation)` -- create a child allocator with its own name, initial reservation, and memory cap
- `newChildAllocator(String name, AllocationListener listener, long initialReservation, long maxAllocation)` -- same but with an `AllocationListener` for monitoring
- `close()` -- close the allocator, releasing all resources; throws if buffers are still outstanding (leak detection)
- `getAllocatedMemory()` -- returns current allocated memory in bytes
- `getLimit()` -- returns the memory limit
- `setLimit(long newLimit)` -- dynamically change the memory limit
- `isOverLimit()` -- check if the allocator has exceeded its limit
- `getName()` -- get the allocator name (useful for debugging)
- `wrapForeignAllocation(ForeignAllocation)` -- account for memory allocated outside Arrow

**Guarantees:**
- Allocation failure is recoverable (throws `OutOfMemoryException`, not a JVM `OutOfMemoryError`)
- Allocations in child allocators are reflected in all parent allocators (hierarchical accounting)
- On close, outstanding buffers cause an `IllegalStateException` with leak diagnostics

Sources:
- [Arrow Java Memory Management docs](https://arrow.apache.org/docs/18.0/java/memory.html)
- [BufferAllocator Javadoc](https://arrow.apache.org/docs/dev/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/BufferAllocator.html)

### 1.2 RootAllocator

`RootAllocator` is the concrete implementation of `BufferAllocator`. It serves as "the master bookkeeper for all memory allocations" across the JVM.

**Key characteristics:**
- Typically only one is created per JVM or application
- Enforces the program-wide memory limit via its constructor: `new RootAllocator(long limit)` or `new RootAllocator()` (defaults to `Long.MAX_VALUE`)
- All child allocators ultimately roll up accounting to the root
- The `RootAllocator` constructor triggers `AllocationManager.Factory` resolution (classpath scanning to find `arrow-memory-unsafe` or `arrow-memory-netty`)
- Implements `AutoCloseable` -- should be closed when no longer needed

Sources:
- [RootAllocator Javadoc](https://arrow.apache.org/docs/dev/java/reference/org/apache/arrow/memory/RootAllocator.html)

### 1.3 Child Allocators -- Isolation, Accounting, Hierarchy

Child allocators are created via `parentAllocator.newChildAllocator(name, initialReservation, maxAllocation)`.

**Isolation:**
- Each child has its own memory limit, which must be <= parent's remaining capacity
- Child names appear in leak diagnostics, making it easy to trace which subsystem leaked

**Accounting:**
- Every byte allocated in a child is also accounted in all ancestor allocators up to the root
- A child cannot exceed its own limit OR cause any ancestor to exceed its limit

**Hierarchy:**
- Arbitrary nesting depth: root -> child -> grandchild -> ...
- Closing a child verifies that all its buffers and children have been released
- Useful for scoping: e.g. "this query's allocator" as a child of "this connection's allocator"

**Use cases:**
- Setting a lower memory limit for a particular section of code
- Verifying that specific code sections don't leak memory when the child is closed

Sources:
- [Arrow Java Memory Management docs](https://arrow.apache.org/docs/18.0/java/memory.html)

### 1.4 ArrowBuf -- What It Wraps, Lifecycle, Reference Counting

`ArrowBuf` represents "a single, contiguous region of direct memory" consisting of an address and a length. It provides low-level interfaces for working with memory content, similar to `ByteBuffer`.

**What it wraps:**
- A direct (off-heap) memory region with a base address and capacity
- Associated with a `ReferenceManager` (which is a `BufferLedger`) for reference counting
- Associated with an `AllocationManager` that owns the physical memory

**Constructor signature:**
```java
ArrowBuf(ReferenceManager referenceManager, BufferLedger bufferLedger, long capacity, long memoryAddress)
```

**Lifecycle:**
1. Allocated from a `BufferAllocator` via `allocator.buffer(size)`
2. Used via typed accessors: `getByte()`, `getLong()`, `getBytes()`, `setBytes()`, etc.
3. Released via `buf.close()` or `buf.getReferenceManager().release()` which decrements the ref count
4. When ref count reaches zero, the underlying memory is deallocated or returned to pool

**Reference counting:**
- Arrow uses manual reference counting instead of GC-based cleanup
- Each buffer starts with a reference count of 1
- `retain()` increments the count; `release()` decrements it
- Higher-level APIs like `ValueVector` implement `Closeable`/`AutoCloseable` and automatically decrement ref count on close
- Users typically interact with `ValueVector` rather than raw `ArrowBuf`

Sources:
- [ArrowBuf Javadoc](https://arrow.apache.org/docs/18.1/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/ArrowBuf.html)
- [Arrow Java Memory Management docs](https://arrow.apache.org/docs/18.0/java/memory.html)

### 1.5 AllocationManager -- The Factory Abstraction

`AllocationManager` is the abstract class that implements physical memory allocation. It manages the relationship between allocators and a particular memory allocation.

**Abstract methods:**
- `long getSize()` -- returns the size of the underlying memory chunk (may differ from requested size due to rounding)
- `long memoryAddress()` -- returns the absolute memory address of the chunk's first byte
- `void release0()` -- deallocates the underlying memory

**Constructor:**
```java
protected AllocationManager(BufferAllocator accountingAllocator)
```

**Responsibilities:**
- Maintains `BufferLedger` associations with allocators
- Does NOT track reference counts (that is `BufferLedger`/`ReferenceManager`'s job)
- Ensures all memory is accurately accounted from Root's perspective
- Ensures memory is correctly released once all allocators stop using it
- Thread safety: lock-free within single ledger contexts; locks acquired for cross-ledger operations

**Inner interface -- `AllocationManager.Factory`:**
```java
public interface Factory {
    AllocationManager create(BufferAllocator accountingAllocator, long size);
    ArrowBuf empty();
}
```
- `create()` -- creates a new `AllocationManager` for a memory region of the given size
- `empty()` -- returns a zero-length `ArrowBuf` (used as a sentinel/placeholder)

Sources:
- [AllocationManager Javadoc](https://arrow.apache.org/java/main/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/AllocationManager.html)

---

## 2. Available AllocationManager Implementations

### 2.1 arrow-memory-unsafe (sun.misc.Unsafe)

**Module:** `org.apache.arrow:arrow-memory-unsafe`

**Class:** `org.apache.arrow.memory.unsafe.UnsafeAllocationManager`

**How it works:**
- Allocates direct (off-heap) memory using `sun.misc.Unsafe.allocateMemory(size)` and frees it with `Unsafe.freeMemory(address)`
- The simplest possible allocation strategy -- a thin wrapper around raw memory allocation
- No pooling, no buffering -- each allocation is a direct system call

**Tradeoffs:**
- **Pros:** Minimal dependencies (no Netty needed), smaller footprint, simpler code path, well-suited for environments that don't already use Netty
- **Cons:** No memory pooling (every alloc/free is a system call), `sun.misc.Unsafe` is deprecated for removal in JDK 24+ (JEP 498), issues on platforms where Unsafe is unavailable (Android, Alpine Linux in some configurations)
- **Future:** Arrow Java is working on migrating away from `sun.misc.Unsafe` toward `java.lang.foreign` (Panama/FFM API) as a replacement

Sources:
- [UnsafeAllocationManager Javadoc](https://arrow.apache.org/docs/18.1/java/reference/org.apache.arrow.memory.unsafe/org/apache/arrow/memory/unsafe/UnsafeAllocationManager.html)
- [Arrow-java issue #511: Unsafe deprecated for removal](https://github.com/apache/arrow-java/issues/511)

### 2.2 arrow-memory-netty (Netty Pooled Buffers)

**Module:** `org.apache.arrow:arrow-memory-netty`

**Class:** `org.apache.arrow.memory.netty.NettyAllocationManager`

**How it works:**
- Uses Netty's `PooledByteBufAllocator` (specifically a custom `PooledByteBufAllocatorL`) as the inner allocator
- Rounds requested sizes up to the next power of two
- Netty's pool operates through a sophisticated system: memory organized into arenas, chunks, and subpages using algorithms inspired by jemalloc
- Thread-local caches reduce contention for frequently allocated sizes
- When a buffer is requested, the pool divides a pre-allocated chunk logically and returns a sub-region

**Tradeoffs:**
- **Pros:** Memory pooling reduces system call overhead for frequent alloc/free cycles, good for high-throughput scenarios, battle-tested in production Netty deployments
- **Cons:** Heavier dependency (requires Netty), rounds allocations up to power-of-two (potential memory waste), more complex code path, can have version compatibility issues with Netty (e.g., Netty 4.1.120+ broke Arrow)
- **Note:** The actual size returned by `getSize()` is the `allocatedSize` (rounded up), not the `requestedSize`

Sources:
- [Arrow-java issue #123: Netty allocator initialization](https://github.com/apache/arrow-java/issues/123)

### 2.3 How the Default Is Selected

The default `AllocationManager.Factory` is selected by `DefaultAllocationManagerOption`, which uses a combination of environment variables and system properties:

**Configuration hierarchy (highest to lowest priority):**
1. System property: `arrow.allocation.manager.type`
2. Environment variable: `ARROW_ALLOCATION_MANAGER_TYPE`
3. Classpath scanning (fallback)

**AllocationManagerType enum:**
- `Netty` -- selects `NettyAllocationManager`
- `Unsafe` -- selects `UnsafeAllocationManager`

**Classpath scanning mechanism:**
- If no type is explicitly configured, Arrow attempts `Class.forName()` to load `DefaultAllocationManagerFactory` from either:
  - `org.apache.arrow.memory.unsafe.UnsafeAllocationManager` (via its `FACTORY` static field)
  - `org.apache.arrow.memory.netty.NettyAllocationManager` (via its `FACTORY` static field)
- If neither is found, throws `RuntimeException`: "No DefaultAllocationManager found on classpath"

**Key field:**
- `DefaultAllocationManagerOption.DEFAULT_ALLOCATION_MANAGER_FACTORY` -- a static field that holds the resolved `AllocationManager.Factory`. This is the field that ES overrides via reflection.

Sources:
- [DefaultAllocationManagerOption source on Fossies](https://fossies.org/linux/apache-arrow/java/memory/memory-core/src/main/java/org/apache/arrow/memory/DefaultAllocationManagerOption.java)
- [Snowflake JDBC issue #1095](https://github.com/snowflakedb/snowflake-jdbc/issues/1095)

### 2.4 Custom AllocationManager

**Yes, you can provide a custom AllocationManager.** You need to:
1. Implement `AllocationManager.Factory` (with `create()` and `empty()` methods)
2. Either:
   - Set the system property `arrow.allocation.manager.type` + provide a matching `DefaultAllocationManagerFactory` on classpath, OR
   - Use reflection to override `DefaultAllocationManagerOption.DEFAULT_ALLOCATION_MANAGER_FACTORY` (as ES does), OR
   - Pass a custom `AllocationManager.Factory` when constructing `RootAllocator` (newer Arrow versions)

---

## 3. Mechanisms to Change/Customize Allocators

### 3.1 AllocationManager.Factory Interface

```java
public interface AllocationManager.Factory {
    AllocationManager create(BufferAllocator accountingAllocator, long size);
    ArrowBuf empty();
}
```

- `create()` is called every time a buffer needs to be allocated
- `empty()` returns a sentinel zero-length buffer
- Implementing this interface gives full control over how physical memory is acquired and released

### 3.2 DefaultAllocationManagerOption -- How It Works

`DefaultAllocationManagerOption` is a utility class in `arrow-memory-core` that resolves and caches the `AllocationManager.Factory`:

1. On first access, `getDefaultAllocationManagerFactory()` is called
2. It checks `arrow.allocation.manager.type` system property and `ARROW_ALLOCATION_MANAGER_TYPE` env var
3. Based on the type, it loads the corresponding factory class via reflection
4. The result is stored in `DEFAULT_ALLOCATION_MANAGER_FACTORY` (a static field)
5. All `RootAllocator` instances subsequently use this cached factory

### 3.3 How ES Overrides It (AllocationManagerShim)

ES implements a custom override via `AllocationManagerShim` (file: `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java`).

**What it does:**

`AllocationManagerShim` implements `AllocationManager.Factory` and **always throws `UnsupportedOperationException`** on both `create()` and `empty()`:

```java
// AllocationManagerShim.java:61-63
@Override
public AllocationManager create(BufferAllocator accountingAllocator, long size) {
    throw new UnsupportedOperationException("Arrow memory manager is disabled");
}

@Override
public ArrowBuf empty() {
    throw new UnsupportedOperationException("Arrow memory manager is disabled");
}
```

**Why:** ES does not actually use Arrow's memory manager for its ESQL Arrow output path. It streams dataframe buffers directly from ESQL blocks. But Arrow's initialization code requires a memory manager to be present.

**How the injection works** (AllocationManagerShim.java:40-58):

```java
@SuppressForbidden(reason = "Inject the default Arrow memory allocation manager")
public static void init() {
    try {
        Class.forName("org.elasticsearch.test.ESTestCase");
        // In tests, don't disable -- use a real allocator for testing
    } catch (ClassNotFoundException notfound) {
        // In production, disable via reflection
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Field field = DefaultAllocationManagerOption.class
                .getDeclaredField("DEFAULT_ALLOCATION_MANAGER_FACTORY");
            field.setAccessible(true);
            field.set(null, new AllocationManagerShim());
            return null;
        });
    }
}
```

**Key details:**
- Uses `AccessController.doPrivileged` for security manager compatibility
- Uses `setAccessible(true)` to override the private static field
- **Test-aware:** Checks if `ESTestCase` is on the classpath. If yes (test environment), it does NOT disable the allocator, allowing tests to use real Arrow allocators. If no (production), it injects the shim.
- Called from `ResponseSegment` static initializer block (ArrowResponse.java:132-134):
  ```java
  static {
      AllocationManagerShim.init();
  }
  ```

File: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java`

### 3.4 AllocationListener -- Hooks for Monitoring/Circuit-Breaking

`AllocationListener` is an interface that can be attached to child allocators to receive callbacks on memory events:

**Methods:**
| Method | Signature | Description |
|--------|-----------|-------------|
| `onPreAllocation` | `void onPreAllocation(long size)` | Called before allocation; can throw to reject |
| `onAllocation` | `void onAllocation(long size)` | Called after allocation; cannot throw |
| `onRelease` | `void onRelease(long size)` | Called when buffer is released; cannot throw |
| `onFailedAllocation` | `boolean onFailedAllocation(long size, AllocationOutcome outcome)` | Called on failure; return true to retry after making space |
| `onChildAdded` | `void onChildAdded(BufferAllocator parent, BufferAllocator child)` | Called when child allocator is created |
| `onChildRemoved` | `void onChildRemoved(BufferAllocator parent, BufferAllocator child)` | Called when child allocator is removed |

**Use for circuit breaking:**
- `onPreAllocation` is the key hook: throw an exception here to terminate the allocation before it happens
- This is how external memory accounting systems can integrate: check available memory in `onPreAllocation`, throw if insufficient
- `onFailedAllocation` allows a second chance: free resources or raise limits, return `true` to retry

**Usage:**
```java
BufferAllocator child = parent.newChildAllocator("monitored", myListener, 0, maxAlloc);
```

**Static instance:** `AllocationListener.NOOP` -- a no-op default

Sources:
- [AllocationListener Javadoc](https://arrow.apache.org/docs/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/AllocationListener.html)
- [ARROW-2696: onFailedAllocation](https://issues.apache.org/jira/browse/ARROW-2696)

### 3.5 Memory Limits on Allocators

**Yes, memory limits can be set on allocators:**

1. **At construction:** `new RootAllocator(long limit)` or `parent.newChildAllocator(name, initReservation, maxAllocation)`
2. **Dynamically:** `allocator.setLimit(long newLimit)` -- change the limit at runtime
3. **Hierarchical enforcement:** A child's allocation fails if it would exceed either its own limit or any ancestor's limit

### 3.6 Integration with External Memory Accounting

**Yes, via two mechanisms:**

1. **AllocationListener:** Attach to child allocators for pre/post allocation hooks. Use `onPreAllocation` to check external limits and throw to reject.

2. **wrapForeignAllocation():** Account for memory allocated outside of Arrow (e.g., C++ memory shared via C Data Interface) so that Arrow's accounting reflects total memory usage.

---

## 4. How ES Uses Allocators Today

### 4.1 AllocationManagerShim -- Disabling Arrow's Allocator

File: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java`

As detailed in section 3.3, ES disables Arrow's memory allocator entirely in production. The rationale (from AllocationManagerShim.java:23-28):

> We don't actually use Arrow's memory manager as we stream dataframe buffers directly from ESQL blocks.
> But Arrow won't initialize properly unless it has one (and requires either the arrow-memory-netty or
> arrow-memory-unsafe libraries). It also does some fancy classpath scanning and calls to setAccessible
> which will be rejected by the security manager.

The `arrow-memory-unsafe` module IS on the classpath (build.gradle:21: `runtimeOnly('org.apache.arrow:arrow-memory-unsafe:18.3.0')`), but the shim prevents it from being used in production. In tests, the real `UnsafeAllocationManager` is used.

### 4.2 RootAllocator in FlightConnector

File: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java`

`FlightConnector` creates a `RootAllocator` directly (FlightConnector.java:55):
```java
this.allocator = new RootAllocator();
```

This is a real, functional allocator (no memory limit -- defaults to `Long.MAX_VALUE`). It is used for:
- Building `FlightClient` instances (FlightConnector.java:56): `FlightClient.builder(allocator, location).build()`
- Shared across all Flight clients in this connector (both default and location-specific)
- Properly closed in `FlightConnector.close()` (FlightConnector.java:125): `allocator.close()` in a `finally` block, after all clients are closed

The same pattern appears in:
- **FlightConnectorFactory.java:54** -- ephemeral `RootAllocator` in try-with-resources for schema resolution
- **FlightSplitProvider.java:46** -- ephemeral `RootAllocator` in try-with-resources for split discovery

### 4.3 RootAllocator in IcebergSourceOperatorFactory

File: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`

Creates a `RootAllocator` with explicit max limit (IcebergSourceOperatorFactory.java:167):
```java
BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
```

Used for Arrow vectorized reading of Iceberg/Parquet data. Closed in the `ColumnarBatchToVectorSchemaRootIterable`'s iterator close method (IcebergSourceOperatorFactory.java:226-228):
```java
arrowReader.close();
allocator.close();
```

Note: The `get(DriverContext)` method currently throws `UnsupportedOperationException` (line 103) -- this is a work-in-progress, with Iceberg currently providing only schema discovery functionality.

### 4.4 ArrowResponse Dummy ArrowBuf Approach

File: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/BlockConverter.java`

This is the most interesting pattern. ES completely avoids Arrow's memory allocator for its primary use case (ESQL Arrow output) by using **dummy ArrowBuf instances** (BlockConverter.java:417-419):

```java
private static ArrowBuf dummyArrowBuf(long size) {
    return new ArrowBuf(null, null, 0, 0).writerIndex(size);
}
```

**How it works:**
1. The dummy `ArrowBuf` is constructed with `null` for both `ReferenceManager` and `BufferLedger`, zero capacity, and zero memory address
2. Only the `writerIndex` is set to the desired size
3. These dummy bufs are added to the `ArrowRecordBatch` constructor so Arrow can compute offsets and buffer sizes for the IPC format
4. **Actual data writing** is done by separate `BufWriter` closures that write directly to ES's `RecyclerBytesStreamOutput`
5. The `WriteChannel` is overridden (ArrowResponse.java:276-305) to intercept `write(ArrowBuf)` calls and delegate to the corresponding `BufWriter` instead of reading from the ArrowBuf

**Architecture of the bypass:**
```
Normal Arrow path:  ESQL Block -> Arrow ValueVector (ArrowBuf memory) -> IPC serialization
ES's path:          ESQL Block -> dummy ArrowBuf (size only) + BufWriter closures -> direct IPC serialization
```

This completely avoids:
- Allocating Arrow memory for data buffers
- Copying data from ESQL blocks to Arrow buffers
- Arrow reference counting overhead
- Any dependency on a functioning `AllocationManager`

The `ArrowRecordBatch` is created with `retainBuffers=false` (ArrowResponse.java:339) so Arrow does not try to increment reference counts on the dummy bufs.

### 4.5 Build Configuration -- Arrow Version and Dependencies

ES uses Arrow 18.3.0 across all modules:
- `esql/arrow/build.gradle:17-18,21` -- `arrow-vector`, `arrow-format`, `arrow-memory-core` as implementation; `arrow-memory-unsafe` as runtimeOnly
- `esql-datasource-grpc/build.gradle:31,39,47-48` -- `flight-core`, `arrow-vector`, `arrow-memory-core` as implementation; `arrow-memory-unsafe` as runtimeOnly
- `esql-datasource-iceberg/build.gradle:115-116` -- `arrow-vector`, `arrow-memory-core` as compileOnly (runtime provided by esql arrow module)

All modules use `--add-opens=java.base/java.nio=ALL-UNNAMED` for tests to permit Arrow's reflective access.

---

## 5. Arrow Memory Lifecycle

### 5.1 How Buffers Are Allocated, Tracked, and Freed

**Allocation flow:**
1. User calls `allocator.buffer(size)` on a `BufferAllocator`
2. The allocator checks limits (own + all ancestors)
3. If allowed, calls `AllocationManager.Factory.create(allocator, size)` to get physical memory
4. An `AllocationManager` is created, which allocates the raw memory (via Unsafe or Netty)
5. A `BufferLedger` (which is a `ReferenceManager`) is created to track the buffer
6. An `ArrowBuf` is returned wrapping the memory, with ref count = 1

**Tracking:**
- Each `ArrowBuf` has an associated `ReferenceManager` (retrievable via `buf.getReferenceManager()`)
- The `BufferLedger` maintains the ref count and the association between allocator and `AllocationManager`
- Multiple `ArrowBuf` instances can reference the same `AllocationManager` (via slicing)
- The allocator maintains its total allocated memory count

**Freeing:**
1. User calls `buf.close()` or `buf.getReferenceManager().release()`
2. Ref count is decremented
3. If ref count reaches zero, `AllocationManager.release0()` is called to free physical memory
4. The allocator's accounting is updated (allocated bytes decremented)

### 5.2 What Happens on Allocator Close (Leak Detection)

**Normal mode:**
- `allocator.close()` checks for outstanding buffers/children
- If any are found, logs "Memory was leaked by query" and throws `IllegalStateException`
- The allocator releases excess memory to its parent before throwing

**Debug mode** (enabled via `-Darrow.memory.debug.allocator=true`):
- Records full stack traces for every allocation, retain, and release operation
- On close with leaks, the error includes detailed information about:
  - Which buffers are still outstanding
  - Where they were allocated (stack trace)
  - Which child allocators are still open
  - Full ledger state
- `ArrowBuf.print()` can be used during debugging to obtain a debug string with allocation stack traces

### 5.3 Reference Counting Semantics

- Each `ArrowBuf` starts with ref count = 1
- `retain()` / `retain(int increment)` -- increment ref count
- `release()` / `release(int decrement)` -- decrement ref count
- When count reaches zero, memory is freed
- **Slicing:** `buf.slice(offset, length)` returns a new `ArrowBuf` that shares the same underlying memory. The slice has its own ref count but shares the `AllocationManager`.
- **Important:** ref counting is manual -- the user is responsible for balancing retains and releases. This is by design, as Arrow avoids GC-based cleanup for deterministic memory management.

### 5.4 Transfer Between Allocators

**Transfer operation:**
- `buf.getReferenceManager().transferOwnership(targetAllocator)` creates a new `ArrowBuf` associated with the target allocator
- Memory ownership (accounting) moves from the source to the target allocator
- If this is the first association with the target allocator, the new `ArrowBuf` has ref count = 1
- If the target already had an association, ref count = current + 1

**Key behavior:**
- `ArrowBuf.release()` is what actually causes memory ownership transfer between `BufferLedger`s
- Ownership transfer always proceeds, even if it violates an allocator limit -- this is by design to prevent memory leaks
- The `AllocationManager` mediates the transfer: it knows about all `BufferLedger` instances referencing its memory
- Cross-ledger operations acquire locks on the `AllocationManager` instance; with thousands of instances per query, contention is minimal

Sources:
- [Arrow Java Memory Management docs](https://arrow.apache.org/docs/18.0/java/memory.html)
- [ReferenceManager Javadoc](https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html)

---

## Summary: ES's Three Distinct Patterns

| Pattern | Where | Allocator Used? | Purpose |
|---------|-------|-----------------|---------|
| **AllocationManagerShim** | ESQL Arrow output (ArrowResponse) | No -- disabled, dummy ArrowBufs | Stream ESQL blocks directly as Arrow IPC without Arrow memory |
| **Real RootAllocator** | FlightConnector, FlightSplitProvider, FlightConnectorFactory | Yes -- full Arrow memory | Arrow Flight gRPC protocol requires real allocators |
| **Real RootAllocator** | IcebergSourceOperatorFactory | Yes -- full Arrow memory | Iceberg ArrowReader for vectorized Parquet reading |

The key insight is that ES's primary Arrow usage (ESQL query output) deliberately avoids Arrow's memory management entirely, using a clever interception pattern with dummy ArrowBufs and custom WriteChannel overrides. The real Arrow allocators are only used where external Arrow libraries (Flight, Iceberg) require them.

---

## File Citations (Elasticsearch Codebase)

| File | Lines | Content |
|------|-------|---------|
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | 23-28 | Javadoc explaining why the shim exists |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | 33 | `implements AllocationManager.Factory` |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | 40-58 | `init()` method with reflection injection and test detection |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | 61-63 | `create()` throws UnsupportedOperationException |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | 66-68 | `empty()` throws UnsupportedOperationException |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowResponse.java` | 132-134 | `AllocationManagerShim.init()` called in static initializer |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowResponse.java` | 267-269 | Comment: dummy bufs for size tracking, not data |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowResponse.java` | 276-305 | Custom `WriteChannel` overriding `write(ArrowBuf)` to use `BufWriter` closures |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowResponse.java` | 332-340 | `ArrowRecordBatch` creation with `retainBuffers=false` |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/BlockConverter.java` | 417-419 | `dummyArrowBuf()` -- `new ArrowBuf(null, null, 0, 0).writerIndex(size)` |
| `x-pack/plugin/esql/arrow/build.gradle` | 17-21 | Arrow 18.3.0 deps, `arrow-memory-unsafe` as runtimeOnly |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java` | 55 | `this.allocator = new RootAllocator()` |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java` | 56 | `FlightClient.builder(allocator, location).build()` |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java` | 125 | `allocator.close()` in finally block |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnectorFactory.java` | 54-55 | Ephemeral `RootAllocator` for schema resolution |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplitProvider.java` | 46 | Ephemeral `RootAllocator` for split discovery |
| `x-pack/plugin/esql-datasource-grpc/build.gradle` | 47-48 | `arrow-memory-core` + `arrow-memory-unsafe` for grpc module |
| `x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` | 167 | `new RootAllocator(Long.MAX_VALUE)` |
| `x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` | 226-228 | allocator.close() in iterator close |
| `x-pack/plugin/esql-datasource-iceberg/build.gradle` | 115-116 | Arrow compileOnly deps (runtime from esql arrow module) |

## Web Sources

- [Arrow Java Memory Management (v18.0)](https://arrow.apache.org/docs/18.0/java/memory.html)
- [AllocationManager Javadoc](https://arrow.apache.org/java/main/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/AllocationManager.html)
- [BufferAllocator Javadoc](https://arrow.apache.org/docs/dev/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/BufferAllocator.html)
- [RootAllocator Javadoc](https://arrow.apache.org/docs/dev/java/reference/org/apache/arrow/memory/RootAllocator.html)
- [AllocationListener Javadoc](https://arrow.apache.org/docs/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/AllocationListener.html)
- [ArrowBuf Javadoc](https://arrow.apache.org/docs/18.1/java/reference/org.apache.arrow.memory.core/org/apache/arrow/memory/ArrowBuf.html)
- [UnsafeAllocationManager Javadoc](https://arrow.apache.org/docs/18.1/java/reference/org.apache.arrow.memory.unsafe/org/apache/arrow/memory/unsafe/UnsafeAllocationManager.html)
- [DefaultAllocationManagerOption source](https://fossies.org/linux/apache-arrow/java/memory/memory-core/src/main/java/org/apache/arrow/memory/DefaultAllocationManagerOption.java)
- [Arrow-java issue #511: Unsafe deprecated](https://github.com/apache/arrow-java/issues/511)
- [Arrow-java issue #123: Netty allocator](https://github.com/apache/arrow-java/issues/123)
- [ARROW-2696: onFailedAllocation](https://issues.apache.org/jira/browse/ARROW-2696)
- [Apache Arrow Java GitHub](https://github.com/apache/arrow-java)
