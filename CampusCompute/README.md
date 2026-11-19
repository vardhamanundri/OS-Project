# CampusCompute

Command-line prototype for the Operating Systems (UE25CS242A) Level 3 Mini-Project.
Team: PES1UG25AM440, PES1UG25AM488 | Language: C (GNU11) | Platform: Ubuntu / WSL Ubuntu

## Prerequisites
    sudo apt update && sudo apt install -y build-essential zip unzip

## Build / Run / Clean
    make                     # build ./campuscompute from source
    ./campuscompute help     # list all commands
    ./campuscompute demo     # integrated run with PASS/FAIL summary
    ./campuscompute cleanup  # remove ONLY this program's shared memory / semaphores
    make clean               # delete the executable and campuscompute.log

## Commands (each feature is independently testable)
| Command | Requirement |
|---|---|
| `sched fcfs\|rr\|priority\|compare [-q N] [-f FILE]` | FR1, FR2 - job model and CPU scheduling (default quantum 3) |
| `logger [PRODUCERS CONSUMERS EVENTS]` | FR3 - bounded producer-consumer logger, writes `campuscompute.log` |
| `ipc` | FR4 - POSIX shared memory + named semaphores, parent and child |
| `banker safe\|unsafe\|file PATH` | FR5 - Banker's safety algorithm |
| `worker normal\|fail\|crash\|all` | FR6 - fork() / waitpid(); failures are SIMULATED |
| `rlimit nofile\|cpu\|all` | FR7 - setrlimit(RLIMIT_NOFILE / RLIMIT_CPU) |
| `cleanup` | FR7 - remove program-created IPC objects and verify |
| `demo` | FR8 - integrated demonstration |

Invalid commands or input print an `error:` line (and usage where relevant) and return a non-zero exit code.

## Design notes
* **Job model (FR1):** id, arrival, burst, priority, status (NEW -> READY -> RUNNING -> DONE).
  Input is validated (integers in range, unique IDs, 4 fields per line).
* **Tie-breaking:** FCFS and Round Robin - equal arrival: smaller job ID first.
  Priority - smaller number = higher priority; ties broken by earlier arrival, then smaller ID.
* **Round Robin:** default quantum 3 (`-q N`). Jobs arriving during a slice are queued before the preempted job.
* **Job file format:** `id arrival burst priority` per line, `#` comments allowed (see `samples/jobs.txt`).
* **Logger (FR3):** queue capacity 4; one mutex protects the queue, `not_full` / `not_empty` condition
  variables block producers / consumers; a second mutex serialises console and log output.
  Consumers exit once the queue is empty and producers are finished; every thread is joined.
* **IPC (FR4):** objects `/campuscompute_shm`, `/campuscompute_sem_data`, `/campuscompute_sem_ack`
  (visible under `/dev/shm`). Parent sends a number, child replies with its square. All three objects
  are unlinked at the end (also on SIGINT/SIGTERM), and only these names are ever touched.
* **Banker (FR5):** Need = Maximum - Allocation. File format: `n m`, Available, Allocation rows,
  Maximum rows (see `samples/banker_*.txt`).
* **Workers (FR6):** three kinds - normal exit 0, SIMULATED exit code 3, SIMULATED SIGTERM. The parent
  reaps them with `waitpid()` and decodes the status with `WIFEXITED` / `WIFSIGNALED`.
* **Resource limits (FR7):** run in a child so the parent is unaffected. `RLIMIT_NOFILE` lowers the
  open-file limit and `open()` then fails with EMFILE; `RLIMIT_CPU` (1 s soft) makes the kernel send SIGXCPU.
* SIGPIPE is ignored, so piping output to `head` / `tail` never skips cleanup.
