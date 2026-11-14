![Screenshot from 2023-04-12 23-00-25](https://user-images.githubusercontent.com/69158213/231595436-b95247d6-3d2c-4ae9-8f08-46caed5f8a65.png)

## SPSC latency perf harness

Use `com.abidi.runner.SPSCLatencyPerfMain` to run one producer and one consumer forever over `SPSCLockFreeCircularQueue`.

Build:

```bash
mvn clean test
```

Run with default queue size (`65536`, power of two):

```bash
mvn -q -DskipTests exec:java -Dexec.mainClass=com.abidi.runner.SPSCLatencyPerfMain
```

Run with a custom queue size (example `262144`):

```bash
mvn -q -DskipTests exec:java -Dexec.mainClass=com.abidi.runner.SPSCLatencyPerfMain -Dexec.args="262144"
```

Capture lock/CPU costs with Linux `perf`:

```bash
perf stat -d -p <java_pid>
perf record -F 999 -g -p <java_pid> -- sleep 30
perf report
```

