# Detached fuzz campaigns

A terminal or tool-session reconnect can kill its child processes. For long
runs, use the Docker daemon to supervise the campaign. The environment and
daemon must remain available for the entire interval.

Build the runtime image with a trusted CA bundle:

```sh
docker build --secret id=ca_bundle,src=/etc/ssl/certs/ca-certificates.crt \
  -t partitionline-broker-fuzz-runtime -f fuzz/broker/Dockerfile fuzz/broker
```

The image supplies Python, Git and the C++ runtime. Compile the instrumented
executables with stable Rust using the [runner instructions](README.md), then
mount the repository and build directory read-only at their original absolute
paths. Mount a separate writable output parent at its original path. Keeping
paths unchanged lets the runner verify its frozen build and source inputs.

Run with `--detach --init --network none --restart=no`. The init process keeps
the Python supervisor above PID 1 and reaps exited children. Set explicit CPU,
memory and PID limits appropriate to the host. Run the nine real supervisor
checks inside the image before starting the 24-hour attempt.

Retain the exact image ID, container ID, launch arguments, limits and inspected
state with the job record. Use `docker logs` and the output's `status.json` for
observations. Use `docker stop --time 15 <container>` for joined shutdown.

A stale heartbeat or a missing final receipt is incomplete, even if an old
status file says `running`. A stopped container never counts as a completed
campaign. A fresh attempt must complete its full interval; interrupted time
is not carried forward. Containerization supplies process supervision here.
It does not qualify controlled benchmark hardware or a production deployment.
