/*
 * CampusCompute - Intelligent Job Scheduling and Resource Management
 * Operating Systems (UE25CS242A) - Level 3 Mini-Project
 * Authors: PES1UG25AM440, PES1UG25AM488
 * Build  : make            (gcc -Wall -Wextra -std=gnu11 -pthread, links -lrt)
 *
 * Features (one sub-command each, so every feature is independently testable):
 *   sched   FR1/FR2  FCFS, Round Robin, non-preemptive Priority, comparison
 *   logger  FR3      bounded producer-consumer logger (pthreads, mutex + condvars)
 *   ipc     FR4      POSIX shared memory + named semaphores (parent <-> child)
 *   banker  FR5      Banker's safety algorithm (safe / unsafe / from file)
 *   worker  FR6      fork() + waitpid() failure detection (failures are SIMULATED)
 *   rlimit  FR7      setrlimit(): RLIMIT_NOFILE and RLIMIT_CPU
 *   cleanup FR7      remove only the IPC objects this program creates
 *   demo    FR8      integrated run with PASS/FAIL summary
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>

/* ------------------------------------------------------------------ */
/* Limits and names                                                    */
/* ------------------------------------------------------------------ */
#define MAX_JOBS        64
#define MAX_BURST       10000
#define MAX_ARRIVAL     100000
#define MAX_PRIORITY    1000
#define DEFAULT_QUANTUM 3

#define LOG_QUEUE_CAP   4
#define LOG_FILE        "campuscompute.log"

#define SHM_NAME        "/campuscompute_shm"
#define SEM_DATA_NAME   "/campuscompute_sem_data"
#define SEM_ACK_NAME    "/campuscompute_sem_ack"
#define SHM_PATH        "/dev/shm/campuscompute_shm"
#define SEM_DATA_PATH   "/dev/shm/sem.campuscompute_sem_data"
#define SEM_ACK_PATH    "/dev/shm/sem.campuscompute_sem_ack"

#define MAXP 10
#define MAXR 10

/* ------------------------------------------------------------------ */
/* Small helpers                                                       */
/* ------------------------------------------------------------------ */
static void banner(const char *title)
{
    printf("\n=== %s ===\n", title);
}

/* Strict integer parser: whole string must be a number within [lo, hi]. */
static int parse_int(const char *s, long lo, long hi, long *out)
{
    char *end = NULL;
    if (s == NULL || *s == '\0')
        return -1;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < lo || v > hi)
        return -1;
    *out = v;
    return 0;
}

static int file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

/* ================================================================== */
/* FR1 / FR2 : jobs and CPU scheduling                                 */
/* ================================================================== */
typedef enum { ST_NEW, ST_READY, ST_RUNNING, ST_DONE } JobStatus;
typedef enum { POL_FCFS, POL_RR, POL_PRIORITY } Policy;

typedef struct {
    int id, arrival, burst, priority;
    int remaining, start, completion, turnaround, waiting;
    JobStatus status;
} Job;

typedef struct { int job_id; int start; int end; } Segment;   /* job_id 0 = CPU idle */
typedef struct { Segment *seg; int nseg; } Timeline;

typedef struct {
    double avg_wt, avg_tat;
    int makespan, switches;
} SchedStats;

static const Job DEFAULT_JOBS[] = {
    /* id arr burst prio */
    {1, 0, 5, 2, 0, 0, 0, 0, 0, ST_NEW},
    {2, 1, 3, 1, 0, 0, 0, 0, 0, ST_NEW},
    {3, 2, 8, 4, 0, 0, 0, 0, 0, ST_NEW},
    {4, 3, 6, 1, 0, 0, 0, 0, 0, ST_NEW},
    {5, 4, 2, 3, 0, 0, 0, 0, 0, ST_NEW},
};
#define DEFAULT_JOB_COUNT 5

static const char *status_name(JobStatus s)
{
    switch (s) {
    case ST_NEW:     return "NEW";
    case ST_READY:   return "READY";
    case ST_RUNNING: return "RUNNING";
    default:         return "DONE";
    }
}

static const char *policy_name(Policy p)
{
    return p == POL_FCFS ? "FCFS" : p == POL_RR ? "Round Robin" : "Priority (non-preemptive)";
}

/* Sort by arrival time, then by job id (the FCFS / RR tie-breaking rule). */
static int cmp_arrival(const void *a, const void *b)
{
    const Job *x = a, *y = b;
    if (x->arrival != y->arrival) return x->arrival - y->arrival;
    return x->id - y->id;
}

static void tl_add(Timeline *tl, int id, int s, int e)
{
    if (e <= s) return;
    if (tl->nseg > 0) {                      /* merge back-to-back slices of one job */
        Segment *l = &tl->seg[tl->nseg - 1];
        if (l->job_id == id && l->end == s) { l->end = e; return; }
    }
    tl->seg[tl->nseg].job_id = id;
    tl->seg[tl->nseg].start = s;
    tl->seg[tl->nseg].end = e;
    tl->nseg++;
}

/* Parse a job file: "id arrival burst priority" per line, '#' starts a comment. */
static int load_jobs(const char *path, Job *out, int *n)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "error: cannot open job file '%s': %s\n", path, strerror(errno));
        return -1;
    }
    char line[256];
    int lineno = 0, count = 0, rc = 0;
    while (fgets(line, sizeof line, f)) {
        lineno++;
        char *hash = strchr(line, '#');
        if (hash) *hash = '\0';
        char *tok[6];
        int nt = 0;
        char *save = NULL;
        for (char *t = strtok_r(line, " \t\r\n", &save); t && nt < 6; t = strtok_r(NULL, " \t\r\n", &save))
            tok[nt++] = t;
        if (nt == 0) continue;
        long v[4];
        if (nt != 4) {
            fprintf(stderr, "error: %s:%d: expected 4 fields (id arrival burst priority), found %d\n", path, lineno, nt);
            rc = -1; break;
        }
        static const long lo[4] = {1, 0, 1, 0};
        long hi[4] = {MAX_JOBS * 1000L, MAX_ARRIVAL, MAX_BURST, MAX_PRIORITY};
        static const char *fname[4] = {"id", "arrival", "burst", "priority"};
        for (int k = 0; k < 4; k++) {
            if (parse_int(tok[k], lo[k], hi[k], &v[k]) != 0) {
                fprintf(stderr, "error: %s:%d: invalid %s '%s' (must be an integer %ld..%ld)\n",
                        path, lineno, fname[k], tok[k], lo[k], hi[k]);
                rc = -1; break;
            }
        }
        if (rc) break;
        if (count >= MAX_JOBS) {
            fprintf(stderr, "error: %s:%d: too many jobs (max %d)\n", path, lineno, MAX_JOBS);
            rc = -1; break;
        }
        for (int i = 0; i < count; i++)
            if (out[i].id == (int)v[0]) {
                fprintf(stderr, "error: %s:%d: duplicate job id %ld\n", path, lineno, v[0]);
                rc = -1; break;
            }
        if (rc) break;
        out[count] = (Job){(int)v[0], (int)v[1], (int)v[2], (int)v[3], 0, 0, 0, 0, 0, ST_NEW};
        count++;
    }
    fclose(f);
    if (rc == 0 && count == 0) {
        fprintf(stderr, "error: '%s' contains no jobs\n", path);
        rc = -1;
    }
    *n = count;
    return rc;
}

static void print_job_table(const Job *j, int n)
{
    printf("Input jobs:\n  %-4s %-8s %-6s %-8s\n", "ID", "Arrival", "Burst", "Priority");
    for (int i = 0; i < n; i++)
        printf("  J%-3d %-8d %-6d %-8d\n", j[i].id, j[i].arrival, j[i].burst, j[i].priority);
}

/*
 * Simulate one policy on a private copy of the job set.
 * verbose != 0 prints the timeline and the metrics table.
 */
static void sched_run(Policy pol, const Job *in, int n, int quantum, int verbose, SchedStats *st)
{
    Job *j = malloc(sizeof(Job) * n);
    int total_burst = 0;
    memcpy(j, in, sizeof(Job) * n);
    qsort(j, n, sizeof(Job), cmp_arrival);
    for (int i = 0; i < n; i++) {
        j[i].remaining = j[i].burst;
        j[i].start = -1;
        j[i].status = ST_NEW;
        total_burst += j[i].burst;
    }
    Timeline tl = { malloc(sizeof(Segment) * (total_burst + n + 2)), 0 };
    char notes[2048] = "";
    size_t noff = 0;
    int t = 0, done = 0;

    if (pol == POL_FCFS) {
        for (int i = 0; i < n; i++) {
            if (t < j[i].arrival) { tl_add(&tl, 0, t, j[i].arrival); t = j[i].arrival; }
            j[i].status = ST_RUNNING;
            j[i].start = t;
            t += j[i].burst;
            j[i].remaining = 0;
            j[i].completion = t;
            j[i].status = ST_DONE;
            tl_add(&tl, j[i].id, j[i].start, t);
        }
    } else if (pol == POL_PRIORITY) {
        while (done < n) {
            int best = -1;
            for (int i = 0; i < n; i++) {
                if (j[i].status == ST_DONE || j[i].arrival > t) continue;
                if (best < 0 || j[i].priority < j[best].priority ||
                    (j[i].priority == j[best].priority && j[i].arrival < j[best].arrival) ||
                    (j[i].priority == j[best].priority && j[i].arrival == j[best].arrival && j[i].id < j[best].id))
                    best = i;
            }
            if (best < 0) {                                  /* CPU idle until next arrival */
                int next = -1;
                for (int i = 0; i < n; i++)
                    if (j[i].status != ST_DONE && (next < 0 || j[i].arrival < j[next].arrival)) next = i;
                tl_add(&tl, 0, t, j[next].arrival);
                t = j[next].arrival;
                continue;
            }
            for (int i = 0; i < n; i++)                       /* record tie-break decisions */
                if (i != best && j[i].status != ST_DONE && j[i].arrival <= t && j[i].priority == j[best].priority
                    && noff < sizeof notes - 128)
                    noff += snprintf(notes + noff, sizeof notes - noff,
                                     "  t=%d: J%d and J%d both have priority %d -> J%d chosen (earlier arrival, then smaller ID)\n",
                                     t, j[best].id, j[i].id, j[best].priority, j[best].id);
            j[best].status = ST_RUNNING;
            j[best].start = t;
            t += j[best].burst;
            j[best].remaining = 0;
            j[best].completion = t;
            j[best].status = ST_DONE;
            tl_add(&tl, j[best].id, j[best].start, t);
            done++;
        }
    } else { /* Round Robin */
        int *queue = malloc(sizeof(int) * (total_burst + n + 2));
        int qh = 0, qt = 0, next = 0;
        while (done < n) {
            while (next < n && j[next].arrival <= t) { j[next].status = ST_READY; queue[qt++] = next++; }
            if (qh == qt) {                                   /* idle: jump to next arrival */
                tl_add(&tl, 0, t, j[next].arrival);
                t = j[next].arrival;
                continue;
            }
            int c = queue[qh++];
            int run = j[c].remaining < quantum ? j[c].remaining : quantum;
            if (j[c].start < 0) j[c].start = t;
            j[c].status = ST_RUNNING;
            tl_add(&tl, j[c].id, t, t + run);
            t += run;
            j[c].remaining -= run;
            /* jobs that arrive during the slice are queued BEFORE the preempted job */
            while (next < n && j[next].arrival <= t) { j[next].status = ST_READY; queue[qt++] = next++; }
            if (j[c].remaining > 0) { j[c].status = ST_READY; queue[qt++] = c; }
            else { j[c].completion = t; j[c].status = ST_DONE; done++; }
        }
        free(queue);
    }

    double sum_wt = 0, sum_tat = 0;
    for (int i = 0; i < n; i++) {
        j[i].turnaround = j[i].completion - j[i].arrival;
        j[i].waiting = j[i].turnaround - j[i].burst;
        sum_wt += j[i].waiting;
        sum_tat += j[i].turnaround;
    }
    int runs = 0;
    for (int i = 0; i < tl.nseg; i++) if (tl.seg[i].job_id) runs++;
    st->avg_wt = sum_wt / n;
    st->avg_tat = sum_tat / n;
    st->makespan = tl.nseg ? tl.seg[tl.nseg - 1].end : 0;
    st->switches = runs > 0 ? runs - 1 : 0;

    if (verbose) {
        char title[96];
        snprintf(title, sizeof title, "%s Scheduling", policy_name(pol));
        banner(title);
        if (pol == POL_RR)
            printf("Time quantum: %d   (arrivals during a slice are queued before the preempted job)\n", quantum);
        if (pol == POL_PRIORITY)
            printf("Rule: smaller number = higher priority; tie -> earlier arrival, then smaller job ID\n");
        else
            printf("Tie-breaking rule: equal arrival time -> smaller job ID first\n");
        printf("\nTimeline (CPU time slices):\n");
        for (int i = 0; i < tl.nseg; i++) {
            if (tl.seg[i].job_id)
                printf("  [%3d - %3d]  J%d\n", tl.seg[i].start, tl.seg[i].end, tl.seg[i].job_id);
            else
                printf("  [%3d - %3d]  (CPU idle)\n", tl.seg[i].start, tl.seg[i].end);
        }
        printf("Execution order: ");
        int first = 1;
        for (int i = 0; i < tl.nseg; i++) {
            if (!tl.seg[i].job_id) continue;
            printf("%sJ%d", first ? "" : " -> ", tl.seg[i].job_id);
            first = 0;
        }
        printf("\n");
        if (noff) printf("\nTie-break decisions:\n%s", notes);
        printf("\n%-4s %-4s %-5s %-4s | %-4s %-4s %-4s | %s\n", "Job", "AT", "BT", "PR", "CT", "TAT", "WT", "Status");
        for (int i = 0; i < n; i++)
            printf("J%-3d %-4d %-5d %-4d | %-4d %-4d %-4d | %s\n", j[i].id, j[i].arrival, j[i].burst,
                   j[i].priority, j[i].completion, j[i].turnaround, j[i].waiting, status_name(j[i].status));
        printf("\nAverage turnaround time = %.2f\nAverage waiting time    = %.2f\n", st->avg_tat, st->avg_wt);
    }
    free(tl.seg);
    free(j);
}

static int cmd_sched(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "error: missing policy\nusage: campuscompute sched fcfs|rr|priority|compare [-q N] [-f FILE]\n");
        return 2;
    }
    const char *p = argv[2];
    int quantum = DEFAULT_QUANTUM;
    const char *file = NULL;
    for (int i = 3; i < argc; i++) {
        long v;
        if (strcmp(argv[i], "-q") == 0 && i + 1 < argc) {
            if (parse_int(argv[++i], 1, 100, &v) != 0) {
                fprintf(stderr, "error: invalid time quantum '%s' (must be an integer 1..100)\n", argv[i]);
                return 2;
            }
            quantum = (int)v;
        } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
            file = argv[++i];
        } else {
            fprintf(stderr, "error: unknown or incomplete option '%s'\nusage: campuscompute sched fcfs|rr|priority|compare [-q N] [-f FILE]\n", argv[i]);
            return 2;
        }
    }
    Policy pol = POL_FCFS;
    int compare = 0;
    if (strcmp(p, "fcfs") == 0) pol = POL_FCFS;
    else if (strcmp(p, "rr") == 0) pol = POL_RR;
    else if (strcmp(p, "priority") == 0) pol = POL_PRIORITY;
    else if (strcmp(p, "compare") == 0) compare = 1;
    else {
        fprintf(stderr, "error: unknown policy '%s' (use fcfs, rr, priority or compare)\n", p);
        return 2;
    }

    Job jobs[MAX_JOBS];
    int n = DEFAULT_JOB_COUNT;
    if (file) {
        if (load_jobs(file, jobs, &n) != 0) return 1;
    } else {
        memcpy(jobs, DEFAULT_JOBS, sizeof DEFAULT_JOBS);
    }

    SchedStats st;
    if (!compare) {
        print_job_table(jobs, n);
        sched_run(pol, jobs, n, quantum, 1, &st);
        return 0;
    }
    banner("Scheduling Comparison");
    print_job_table(jobs, n);
    printf("\nRound Robin quantum = %d\n\n", quantum);
    printf("%-26s %-10s %-10s %-9s %-8s\n", "Policy", "Avg WT", "Avg TAT", "Makespan", "Switches");
    SchedStats all[3];
    Policy order[3] = {POL_FCFS, POL_RR, POL_PRIORITY};
    int best = 0;
    for (int i = 0; i < 3; i++) {
        sched_run(order[i], jobs, n, quantum, 0, &all[i]);
        printf("%-26s %-10.2f %-10.2f %-9d %-8d\n", policy_name(order[i]), all[i].avg_wt, all[i].avg_tat,
               all[i].makespan, all[i].switches);
        if (all[i].avg_wt < all[best].avg_wt) best = i;
    }
    printf("\nLowest average waiting time on this job set: %s (%.2f)\n", policy_name(order[best]), all[best].avg_wt);
    return 0;
}

/* ================================================================== */
/* FR3 : concurrent logger (bounded buffer, pthreads)                  */
/* ================================================================== */
typedef struct { int producer; int seq; } LogItem;
typedef struct { int id; int items; } ThreadArg;

static LogItem q_buf[LOG_QUEUE_CAP];
static int q_head, q_tail, q_count, producers_done;
static int total_produced, total_consumed;
static pthread_mutex_t q_mtx = PTHREAD_MUTEX_INITIALIZER;      /* protects the queue + counters */
static pthread_cond_t  not_full = PTHREAD_COND_INITIALIZER;     /* producers wait here when full */
static pthread_cond_t  not_empty = PTHREAD_COND_INITIALIZER;    /* consumers wait here when empty */
static pthread_mutex_t out_mtx = PTHREAD_MUTEX_INITIALIZER;     /* protects stdout + log file */
static FILE *log_fp;

static void ts_now(char *buf, size_t n)
{
    struct timespec ts;
    struct tm tmv;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tmv);
    size_t k = strftime(buf, n, "%H:%M:%S", &tmv);
    snprintf(buf + k, n - k, ".%03ld", ts.tv_nsec / 1000000);
}

static void say(const char *fmt, ...)
{
    va_list ap;
    pthread_mutex_lock(&out_mtx);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    fflush(stdout);
    pthread_mutex_unlock(&out_mtx);
}

static void *producer_thread(void *arg)
{
    ThreadArg *a = arg;
    for (int k = 1; k <= a->items; k++) {
        pthread_mutex_lock(&q_mtx);
        while (q_count == LOG_QUEUE_CAP) {
            say("  [producer-%d] queue FULL (%d/%d) -> waiting\n", a->id, q_count, LOG_QUEUE_CAP);
            pthread_cond_wait(&not_full, &q_mtx);
        }
        q_buf[q_tail] = (LogItem){a->id, k};
        q_tail = (q_tail + 1) % LOG_QUEUE_CAP;
        q_count++;
        total_produced++;
        say("  [producer-%d] enqueued event #%d   (queue %d/%d)\n", a->id, k, q_count, LOG_QUEUE_CAP);
        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&q_mtx);
        usleep(15000);
    }
    return NULL;
}

static void *consumer_thread(void *arg)
{
    ThreadArg *a = arg;
    for (;;) {
        pthread_mutex_lock(&q_mtx);
        while (q_count == 0 && !producers_done)
            pthread_cond_wait(&not_empty, &q_mtx);
        if (q_count == 0) {                       /* empty and producers finished -> exit */
            pthread_mutex_unlock(&q_mtx);
            break;
        }
        LogItem it = q_buf[q_head];
        q_head = (q_head + 1) % LOG_QUEUE_CAP;
        q_count--;
        total_consumed++;
        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&q_mtx);

        char ts[32];
        pthread_mutex_lock(&out_mtx);
        ts_now(ts, sizeof ts);               /* stamped under the lock so log lines stay in time order */
        fprintf(log_fp, "[%s] consumer-%d logged event #%d from producer-%d\n", ts, a->id, it.seq, it.producer);
        printf("  [consumer-%d] logged event #%d from producer-%d\n", a->id, it.seq, it.producer);
        fflush(stdout);
        pthread_mutex_unlock(&out_mtx);
        usleep(40000);                            /* slow consumer so the queue really fills */
    }
    return NULL;
}

/* Returns 0 on success (every produced event was consumed and every thread joined). */
static int run_logger(int np, int nc, int items)
{
    pthread_t pt[8], ct[8];
    ThreadArg pa[8], ca[8];
    int joined = 0, started = 0;

    q_head = q_tail = q_count = producers_done = 0;
    total_produced = total_consumed = 0;
    log_fp = fopen(LOG_FILE, "w");
    if (!log_fp) {
        fprintf(stderr, "error: cannot create %s: %s\n", LOG_FILE, strerror(errno));
        return 1;
    }
    banner("Concurrent Logger (bounded producer-consumer)");
    printf("Queue capacity %d, %d producer(s) x %d event(s), %d consumer(s)\n\n", LOG_QUEUE_CAP, np, items, nc);
    fprintf(log_fp, "# CampusCompute log: %d producers x %d events, %d consumers, queue capacity %d\n",
            np, items, nc, LOG_QUEUE_CAP);

    for (int i = 0; i < nc; i++) {
        ca[i] = (ThreadArg){i + 1, 0};
        if (pthread_create(&ct[i], NULL, consumer_thread, &ca[i]) != 0) { perror("pthread_create"); return 1; }
        started++;
    }
    for (int i = 0; i < np; i++) {
        pa[i] = (ThreadArg){i + 1, items};
        if (pthread_create(&pt[i], NULL, producer_thread, &pa[i]) != 0) { perror("pthread_create"); return 1; }
        started++;
    }
    for (int i = 0; i < np; i++) { pthread_join(pt[i], NULL); joined++; }
    pthread_mutex_lock(&q_mtx);
    producers_done = 1;                           /* tell consumers no more items will come */
    pthread_cond_broadcast(&not_empty);
    pthread_mutex_unlock(&q_mtx);
    for (int i = 0; i < nc; i++) { pthread_join(ct[i], NULL); joined++; }

    fprintf(log_fp, "# end: produced=%d consumed=%d\n", total_produced, total_consumed);
    fclose(log_fp);
    log_fp = NULL;

    int ok = (total_produced == np * items && total_consumed == total_produced && joined == started);
    printf("\nThreads joined: %d/%d\n", joined, started);
    printf("Events produced = %d, consumed = %d -> %s\n", total_produced, total_consumed, ok ? "OK" : "MISMATCH");
    printf("Log written to %s\n", LOG_FILE);
    return ok ? 0 : 1;
}

static int cmd_logger(int argc, char **argv)
{
    long np = 2, nc = 2, items = 5;
    if (argc != 2 && argc != 5) {
        fprintf(stderr, "usage: campuscompute logger [PRODUCERS CONSUMERS EVENTS_PER_PRODUCER]\n");
        return 2;
    }
    if (argc == 5 && (parse_int(argv[2], 1, 8, &np) || parse_int(argv[3], 1, 8, &nc) || parse_int(argv[4], 1, 100, &items))) {
        fprintf(stderr, "error: producers and consumers must be 1..8, events 1..100\n");
        return 2;
    }
    return run_logger((int)np, (int)nc, (int)items);
}

/* ================================================================== */
/* FR4 : POSIX shared memory + named semaphores                        */
/* ================================================================== */
typedef struct {
    int  request;
    long reply;
    int  stop;
    char note[48];
} SharedBlock;

#define IPC_ROUNDS 5

/* Remove ONLY the objects this program creates. Returns how many existed. */
static int ipc_unlink_all(void)
{
    int removed = 0;
    if (shm_unlink(SHM_NAME) == 0) removed++;
    if (sem_unlink(SEM_DATA_NAME) == 0) removed++;
    if (sem_unlink(SEM_ACK_NAME) == 0) removed++;
    return removed;
}

static void ipc_report_objects(const char *when)
{
    printf("%s:\n", when);
    printf("  %-38s %s\n", SHM_PATH, file_exists(SHM_PATH) ? "PRESENT" : "absent");
    printf("  %-38s %s\n", SEM_DATA_PATH, file_exists(SEM_DATA_PATH) ? "PRESENT" : "absent");
    printf("  %-38s %s\n", SEM_ACK_PATH, file_exists(SEM_ACK_PATH) ? "PRESENT" : "absent");
}

static int sem_wait_timeout(sem_t *s, int seconds)
{
    struct timespec ts;
    int r;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += seconds;
    while ((r = sem_timedwait(s, &ts)) == -1 && errno == EINTR)
        ;
    return r;
}

static void ipc_signal_handler(int sig)
{
    (void)sig;
    ipc_unlink_all();                  /* keep the system clean if interrupted */
    _exit(130);
}

/* Child: opens the same named objects and answers each request with its square. */
static void ipc_child(void)
{
    sem_t *sd = sem_open(SEM_DATA_NAME, 0);
    sem_t *sa = sem_open(SEM_ACK_NAME, 0);
    int fd = shm_open(SHM_NAME, O_RDWR, 0);
    if (sd == SEM_FAILED || sa == SEM_FAILED || fd < 0) { perror("child open"); _exit(2); }
    SharedBlock *b = mmap(NULL, sizeof *b, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (b == MAP_FAILED) { perror("child mmap"); _exit(2); }
    for (;;) {
        if (sem_wait_timeout(sd, 5) != 0) { perror("child sem_wait"); _exit(3); }
        if (b->stop) break;
        b->reply = (long)b->request * b->request;
        snprintf(b->note, sizeof b->note, "child %d squared %d", (int)getpid(), b->request);
        sem_post(sa);
    }
    munmap(b, sizeof *b);
    close(fd);
    sem_close(sd);
    sem_close(sa);
    _exit(0);
}

/* Returns 0 when every round matched and all IPC objects were released. */
static int run_ipc(void)
{
    int rc = 1, fd = -1;
    SharedBlock *b = MAP_FAILED;
    sem_t *sd = SEM_FAILED, *sa = SEM_FAILED;
    pid_t pid = -1;

    signal(SIGINT, ipc_signal_handler);
    signal(SIGTERM, ipc_signal_handler);
    banner("POSIX Shared Memory + Named Semaphores");
    ipc_unlink_all();                  /* clear stale copies of OUR objects only */

    fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) { perror("shm_open"); goto out; }
    if (ftruncate(fd, sizeof(SharedBlock)) != 0) { perror("ftruncate"); goto out; }
    b = mmap(NULL, sizeof *b, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (b == MAP_FAILED) { perror("mmap"); goto out; }
    memset(b, 0, sizeof *b);
    sd = sem_open(SEM_DATA_NAME, O_CREAT | O_EXCL, 0600, 0);
    sa = sem_open(SEM_ACK_NAME, O_CREAT | O_EXCL, 0600, 0);
    if (sd == SEM_FAILED || sa == SEM_FAILED) { perror("sem_open"); goto out; }

    printf("Created shared memory %s (%zu bytes) and semaphores %s, %s\n",
           SHM_NAME, sizeof(SharedBlock), SEM_DATA_NAME, SEM_ACK_NAME);
    ipc_report_objects("Objects while running");
    fflush(stdout);

    pid = fork();
    if (pid < 0) { perror("fork"); goto out; }
    if (pid == 0) ipc_child();
    printf("\nParent PID %d, child PID %d\n", (int)getpid(), (int)pid);

    int good = 0;
    for (int r = 1; r <= IPC_ROUNDS; r++) {
        int req = r * 7;
        b->request = req;
        sem_post(sd);                                   /* tell child: data ready */
        if (sem_wait_timeout(sa, 5) != 0) { perror("parent sem_wait"); break; }
        printf("  round %d: parent wrote %-3d -> child replied %-4ld (expected %d)  [%s]\n",
               r, req, b->reply, req * req, b->note);
        if (b->reply == (long)req * req) good++;
    }
    b->stop = 1;
    sem_post(sd);
    int status = 0;
    waitpid(pid, &status, 0);
    pid = -1;
    int child_ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf("\nChild exited %s. Rounds verified: %d/%d\n", child_ok ? "normally (code 0)" : "abnormally", good, IPC_ROUNDS);

    rc = (good == IPC_ROUNDS && child_ok) ? 0 : 1;
out:
    if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, NULL, 0); }
    if (b != MAP_FAILED) munmap(b, sizeof *b);
    if (fd >= 0) close(fd);
    if (sd != SEM_FAILED) sem_close(sd);
    if (sa != SEM_FAILED) sem_close(sa);
    int removed = ipc_unlink_all();
    printf("\nCleanup: unlinked %d IPC object(s)\n", removed);
    ipc_report_objects("Objects after cleanup");
    if (file_exists(SHM_PATH) || file_exists(SEM_DATA_PATH) || file_exists(SEM_ACK_PATH)) rc = 1;
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    return rc;
}

static int cmd_cleanup(void)
{
    banner("Cleanup (only objects created by CampusCompute)");
    int removed = ipc_unlink_all();
    printf("Removed %d leftover IPC object(s)\n", removed);
    ipc_report_objects("Verification");
    int clean = !file_exists(SHM_PATH) && !file_exists(SEM_DATA_PATH) && !file_exists(SEM_ACK_PATH);
    printf("Result: %s\n", clean ? "no CampusCompute IPC objects remain" : "some objects remain");
    return clean ? 0 : 1;
}

/* ================================================================== */
/* FR5 : Banker's algorithm                                            */
/* ================================================================== */
typedef struct {
    int n, m;
    int avail[MAXR];
    int alloc[MAXP][MAXR];
    int max[MAXP][MAXR];
    int need[MAXP][MAXR];            /* Need = Maximum - Allocation */
} Banker;

static void print_vec(const int *v, int m)
{
    printf("[");
    for (int k = 0; k < m; k++) printf("%s%d", k ? " " : "", v[k]);
    printf("]");
}

static void banker_compute_need(Banker *b)
{
    for (int i = 0; i < b->n; i++)
        for (int k = 0; k < b->m; k++)
            b->need[i][k] = b->max[i][k] - b->alloc[i][k];
}

static void banker_print(const Banker *b)
{
    printf("Processes n=%d, resource types m=%d\nAvailable = ", b->n, b->m);
    print_vec(b->avail, b->m);
    printf("\n\n%-5s %-14s %-14s %-14s\n", "Proc", "Allocation", "Maximum", "Need");
    for (int i = 0; i < b->n; i++) {
        printf("P%-4d ", i);
        print_vec(b->alloc[i], b->m); printf("%*s", 14 - 2 * b->m, "");
        print_vec(b->max[i], b->m);   printf("%*s", 14 - 2 * b->m, "");
        print_vec(b->need[i], b->m);
        printf("\n");
    }
}

/* Returns 1 if the state is safe (and fills seq), 0 otherwise. */
static int banker_safety(const Banker *b, int *seq)
{
    int work[MAXR], fin[MAXP] = {0}, cnt = 0;
    memcpy(work, b->avail, sizeof(int) * b->m);
    printf("\nSafety algorithm: Work = Available = ");
    print_vec(work, b->m);
    printf("\n");
    while (cnt < b->n) {
        int found = -1;
        for (int i = 0; i < b->n && found < 0; i++) {
            if (fin[i]) continue;
            int ok = 1;
            for (int k = 0; k < b->m; k++)
                if (b->need[i][k] > work[k]) { ok = 0; break; }
            if (ok) found = i;
        }
        if (found < 0) break;
        printf("  P%d: Need ", found); print_vec(b->need[found], b->m);
        printf(" <= Work "); print_vec(work, b->m);
        for (int k = 0; k < b->m; k++) work[k] += b->alloc[found][k];
        printf(" -> can finish, Work becomes "); print_vec(work, b->m); printf("\n");
        fin[found] = 1;
        seq[cnt++] = found;
    }
    if (cnt == b->n) return 1;
    printf("  No remaining process has Need <= Work ");
    print_vec(work, b->m);
    printf("; blocked: ");
    for (int i = 0; i < b->n; i++) if (!fin[i]) printf("P%d ", i);
    printf("\n");
    return 0;
}

/* Runs the check, prints the verdict. Returns 1 safe, 0 unsafe. */
static int banker_report(Banker *b)
{
    int seq[MAXP];
    banker_compute_need(b);
    banker_print(b);
    int safe = banker_safety(b, seq);
    if (safe) {
        printf("\nResult: SAFE state. Safe sequence: ");
        for (int i = 0; i < b->n; i++) printf("P%d%s", seq[i], i + 1 < b->n ? " -> " : "\n");
    } else {
        printf("\nResult: UNSAFE state - no safe sequence exists (deadlock is possible).\n");
    }
    return safe;
}

/* Copy flat n x m data into the fixed-stride MAXP x MAXR matrices. */
static void banker_fill(Banker *b, int n, int m, const int *av, const int *al, const int *mx)
{
    memset(b, 0, sizeof *b);
    b->n = n; b->m = m;
    for (int k = 0; k < m; k++) b->avail[k] = av[k];
    for (int i = 0; i < n; i++)
        for (int k = 0; k < m; k++) {
            b->alloc[i][k] = al[i * m + k];
            b->max[i][k] = mx[i * m + k];
        }
}

static void banker_builtin(Banker *b, int safe_case)
{
    if (safe_case) {                       /* textbook 5 x 3 example */
        const int av[3] = {3, 3, 2};
        const int al[15] = {0,1,0, 2,0,0, 3,0,2, 2,1,1, 0,0,2};
        const int mx[15] = {7,5,3, 3,2,2, 9,0,2, 2,2,2, 4,3,3};
        banker_fill(b, 5, 3, av, al, mx);
    } else {                               /* P3 can finish, then everybody else is stuck */
        const int av[3] = {0, 1, 1};
        const int al[12] = {1,0,0, 0,1,1, 1,1,0, 0,0,1};
        const int mx[12] = {3,2,2, 2,2,2, 3,3,1, 0,0,2};
        banker_fill(b, 4, 3, av, al, mx);
    }
}

/* File format: n m, then Available (m), Allocation (n rows), Maximum (n rows). '#' comments. */
static int banker_load(const char *path, Banker *b)
{
    FILE *f = fopen(path, "r");
    if (!f) { fprintf(stderr, "error: cannot open '%s': %s\n", path, strerror(errno)); return -1; }
    long vals[1024];
    int nv = 0;
    char line[256];
    int rc = 0;
    while (fgets(line, sizeof line, f) && rc == 0) {
        char *h = strchr(line, '#');
        if (h) *h = '\0';
        char *save = NULL;
        for (char *t = strtok_r(line, " \t\r\n", &save); t; t = strtok_r(NULL, " \t\r\n", &save)) {
            long v;
            if (parse_int(t, 0, 100000, &v) != 0 || nv >= 1024) {
                fprintf(stderr, "error: '%s': invalid number '%s' (expected non-negative integers)\n", path, t);
                rc = -1; break;
            }
            vals[nv++] = v;
        }
    }
    fclose(f);
    if (rc) return -1;
    if (nv < 2 || vals[0] < 1 || vals[0] > MAXP || vals[1] < 1 || vals[1] > MAXR) {
        fprintf(stderr, "error: '%s': header must be 'n m' with 1<=n<=%d and 1<=m<=%d\n", path, MAXP, MAXR);
        return -1;
    }
    memset(b, 0, sizeof *b);
    b->n = (int)vals[0]; b->m = (int)vals[1];
    int need_vals = 2 + b->m + 2 * b->n * b->m;
    if (nv != need_vals) {
        fprintf(stderr, "error: '%s': expected %d numbers for n=%d m=%d, found %d\n", path, need_vals, b->n, b->m, nv);
        return -1;
    }
    int p = 2;
    for (int k = 0; k < b->m; k++) b->avail[k] = (int)vals[p++];
    for (int i = 0; i < b->n; i++) for (int k = 0; k < b->m; k++) b->alloc[i][k] = (int)vals[p++];
    for (int i = 0; i < b->n; i++) for (int k = 0; k < b->m; k++) b->max[i][k] = (int)vals[p++];
    for (int i = 0; i < b->n; i++) for (int k = 0; k < b->m; k++)
        if (b->alloc[i][k] > b->max[i][k]) {
            fprintf(stderr, "error: '%s': Allocation exceeds Maximum for P%d, resource %d\n", path, i, k);
            return -1;
        }
    return 0;
}

static int cmd_banker(int argc, char **argv)
{
    Banker b;
    if (argc < 3) {
        fprintf(stderr, "error: missing argument\nusage: campuscompute banker safe|unsafe|file PATH\n");
        return 2;
    }
    if (strcmp(argv[2], "safe") == 0 && argc == 3) banker_builtin(&b, 1);
    else if (strcmp(argv[2], "unsafe") == 0 && argc == 3) banker_builtin(&b, 0);
    else if (strcmp(argv[2], "file") == 0 && argc == 4) { if (banker_load(argv[3], &b) != 0) return 1; }
    else {
        fprintf(stderr, "error: bad arguments\nusage: campuscompute banker safe|unsafe|file PATH\n");
        return 2;
    }
    banner("Banker's Algorithm - Safety Check");
    banker_report(&b);
    return 0;
}

/* ================================================================== */
/* FR6 : worker processes, fork() + waitpid()                          */
/* ================================================================== */
typedef enum { W_NORMAL, W_FAIL, W_CRASH } WorkerKind;

static const char *worker_label(WorkerKind k)
{
    return k == W_NORMAL ? "normal" : k == W_FAIL ? "simulated exit failure" : "simulated crash (signal)";
}

static void worker_child(WorkerKind k)
{
    printf("  [worker %d] started (%s)\n", (int)getpid(), worker_label(k));
    fflush(stdout);
    if (k == W_NORMAL) {
        unsigned long s = 0;
        for (unsigned long i = 1; i <= 2000000UL; i++) s += i % 7;
        printf("  [worker %d] finished its task (checksum %lu), exiting with code 0\n", (int)getpid(), s);
        fflush(stdout);
        _exit(0);
    }
    if (k == W_FAIL) {
        printf("  [worker %d] SIMULATED FAILURE: exiting with code 3\n", (int)getpid());
        fflush(stdout);
        _exit(3);
    }
    printf("  [worker %d] SIMULATED FAILURE: sending itself SIGTERM\n", (int)getpid());
    fflush(stdout);
    raise(SIGTERM);
    _exit(99);                                   /* not reached */
}

/* Prints how the child ended and returns 1 if it matches what was expected. */
static int worker_report(pid_t pid, int status, WorkerKind k)
{
    int match;
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        match = (k == W_NORMAL && code == 0) || (k == W_FAIL && code == 3);
        printf("  parent: pid %d EXITED with code %d%s -> %s\n", (int)pid, code,
               k == W_FAIL ? " (SIMULATED failure, not a real OS fault)" : "",
               code == 0 ? "normal completion" : "abnormal exit detected");
    } else if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        match = (k == W_CRASH && sig == SIGTERM);
        printf("  parent: pid %d KILLED by signal %d (%s)%s -> abnormal termination detected\n", (int)pid, sig,
               strsignal(sig), k == W_CRASH ? " [SIMULATED]" : "");
    } else {
        match = 0;
        printf("  parent: pid %d ended with unexpected status 0x%x\n", (int)pid, status);
    }
    printf("          expected outcome for '%s': %s\n", worker_label(k), match ? "MATCH" : "MISMATCH");
    return match;
}

static int run_workers(int normal, int fail, int crash)
{
    WorkerKind kinds[3];
    int nk = 0, matched = 0;
    if (normal) kinds[nk++] = W_NORMAL;
    if (fail)   kinds[nk++] = W_FAIL;
    if (crash)  kinds[nk++] = W_CRASH;
    pid_t pids[3];
    banner("Worker Processes (fork + waitpid)");
    fflush(stdout);
    for (int i = 0; i < nk; i++) {
        fflush(stdout);                          /* children must not inherit buffered output */
        pids[i] = fork();
        if (pids[i] < 0) { perror("fork"); return 1; }
        if (pids[i] == 0) worker_child(kinds[i]);
        printf("  parent: forked worker pid %d (%s)\n", (int)pids[i], worker_label(kinds[i]));
        fflush(stdout);
    }
    for (int left = nk; left > 0; left--) {
        int status = 0;
        pid_t p = waitpid(-1, &status, 0);
        if (p < 0) { perror("waitpid"); return 1; }
        for (int i = 0; i < nk; i++)
            if (pids[i] == p) matched += worker_report(p, status, kinds[i]);
    }
    printf("\nWorkers reaped: %d, outcomes as expected: %d/%d\n", nk, matched, nk);
    return matched == nk ? 0 : 1;
}

static int cmd_worker(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "error: missing mode\nusage: campuscompute worker normal|fail|crash|all\n");
        return 2;
    }
    if (strcmp(argv[2], "normal") == 0) return run_workers(1, 0, 0);
    if (strcmp(argv[2], "fail") == 0)   return run_workers(0, 1, 0);
    if (strcmp(argv[2], "crash") == 0)  return run_workers(0, 0, 1);
    if (strcmp(argv[2], "all") == 0)    return run_workers(1, 1, 1);
    fprintf(stderr, "error: unknown worker mode '%s'\nusage: campuscompute worker normal|fail|crash|all\n", argv[2]);
    return 2;
}

/* ================================================================== */
/* FR7 : resource limits                                               */
/* ================================================================== */
/* Run the limit in a child so the parent's own limits are untouched. */
static int rlimit_nofile(void)
{
    banner("setrlimit(RLIMIT_NOFILE)");
    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        struct rlimit rl;
        getrlimit(RLIMIT_NOFILE, &rl);
        printf("  child: before: soft=%lu hard=%lu\n", (unsigned long)rl.rlim_cur, (unsigned long)rl.rlim_max);
        rl.rlim_cur = 16;                          /* lowering the soft limit needs no privilege */
        if (setrlimit(RLIMIT_NOFILE, &rl) != 0) { perror("setrlimit"); _exit(2); }
        getrlimit(RLIMIT_NOFILE, &rl);
        printf("  child: after : soft=%lu hard=%lu\n", (unsigned long)rl.rlim_cur, (unsigned long)rl.rlim_max);
        fflush(stdout);
        int fds[64], cnt = 0, err = 0;
        for (; cnt < 64; cnt++) {
            fds[cnt] = open("/dev/null", O_RDONLY);
            if (fds[cnt] < 0) { err = errno; break; }
        }
        printf("  child: opened %d extra file(s) before open() failed with errno=%d (%s)\n", cnt, err, strerror(err));
        for (int i = 0; i < cnt; i++) close(fds[i]);
        fflush(stdout);
        _exit(err == EMFILE ? 0 : 1);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    int ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf("  parent: limit was enforced (open() -> EMFILE): %s\n", ok ? "YES" : "NO");
    return ok ? 0 : 1;
}

static int rlimit_cpu(void)
{
    banner("setrlimit(RLIMIT_CPU)");
    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        struct rlimit rl = {1, 2};                 /* SIGXCPU after 1 CPU-second, SIGKILL after 2 */
        struct rlimit nocore = {0, 0};
        setrlimit(RLIMIT_CORE, &nocore);           /* no core file for the demo */
        if (setrlimit(RLIMIT_CPU, &rl) != 0) { perror("setrlimit"); _exit(2); }
        printf("  child: RLIMIT_CPU set to soft=1s hard=2s, now spinning on the CPU...\n");
        fflush(stdout);
        alarm(10);                                 /* safety net so the demo can never hang */
        volatile unsigned long spin = 0;
        for (;;) spin++;
    }
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int status = 0;
    waitpid(pid, &status, 0);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double el = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    int ok = WIFSIGNALED(status) && WTERMSIG(status) == SIGXCPU;
    if (WIFSIGNALED(status))
        printf("  parent: child killed by signal %d (%s) after %.1f s\n", WTERMSIG(status), strsignal(WTERMSIG(status)), el);
    else
        printf("  parent: child ended unexpectedly (status 0x%x)\n", status);
    printf("  parent: CPU limit enforced by the kernel: %s\n", ok ? "YES" : "NO");
    return ok ? 0 : 1;
}

static int cmd_rlimit(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "error: missing mode\nusage: campuscompute rlimit nofile|cpu|all\n");
        return 2;
    }
    if (strcmp(argv[2], "nofile") == 0) return rlimit_nofile();
    if (strcmp(argv[2], "cpu") == 0)    return rlimit_cpu();
    if (strcmp(argv[2], "all") == 0)    { int a = rlimit_nofile(); int b = rlimit_cpu(); return a | b; }
    fprintf(stderr, "error: unknown rlimit mode '%s'\nusage: campuscompute rlimit nofile|cpu|all\n", argv[2]);
    return 2;
}

/* ================================================================== */
/* FR8 : integrated demo, help                                         */
/* ================================================================== */
static int cmd_demo(void)
{
    struct { const char *name; int ok; } r[10];
    int n = 0;
    banner("CampusCompute Integrated Demonstration");
    char *sargv[] = {"campuscompute", "sched", "compare"};
    r[n].name = "CPU scheduling (FCFS/RR/Priority)"; r[n++].ok = (cmd_sched(3, sargv) == 0);

    r[n].name = "Concurrent logger (pthreads)";        r[n++].ok = (run_logger(2, 2, 4) == 0);
    r[n].name = "Shared memory + semaphore IPC";       r[n++].ok = (run_ipc() == 0);

    Banker b;
    banner("Banker's algorithm: safe case");
    banker_builtin(&b, 1);
    int s1 = banker_report(&b);
    banner("Banker's algorithm: unsafe case");
    banker_builtin(&b, 0);
    int s2 = banker_report(&b);
    r[n].name = "Banker's safe state detected";        r[n++].ok = (s1 == 1);
    r[n].name = "Banker's unsafe state detected";      r[n++].ok = (s2 == 0);

    r[n].name = "Worker normal + simulated failures";  r[n++].ok = (run_workers(1, 1, 1) == 0);
    r[n].name = "Resource limits (setrlimit)";         r[n++].ok = (rlimit_nofile() == 0 && rlimit_cpu() == 0);

    long dummy;
    int bad_rejected = parse_int("abc", 1, 100, &dummy) != 0 && parse_int("0", 1, 100, &dummy) != 0;
    r[n].name = "Invalid input rejected";              r[n++].ok = bad_rejected;
    r[n].name = "IPC cleanup verified";                r[n++].ok = (cmd_cleanup() == 0);

    banner("Demo Summary");
    int failed = 0;
    for (int i = 0; i < n; i++) {
        printf("  [%s] %s\n", r[i].ok ? "PASS" : "FAIL", r[i].name);
        failed += !r[i].ok;
    }
    printf("\n%d/%d checks passed. Log file: %s\n", n - failed, n, LOG_FILE);
    return failed ? 1 : 0;
}

static void usage(FILE *o)
{
    fprintf(o,
        "CampusCompute - job scheduling and resource management prototype\n"
        "usage: campuscompute <command> [options]\n\n"
        "commands:\n"
        "  sched fcfs|rr|priority|compare [-q N] [-f FILE]   CPU scheduling (default quantum %d)\n"
        "  logger [PRODUCERS CONSUMERS EVENTS]               bounded producer-consumer logger\n"
        "  ipc                                               shared memory + named semaphores\n"
        "  banker safe|unsafe|file PATH                      Banker's safety algorithm\n"
        "  worker normal|fail|crash|all                      fork/waitpid failure detection (simulated)\n"
        "  rlimit nofile|cpu|all                             setrlimit() demonstrations\n"
        "  cleanup                                           remove this program's IPC objects\n"
        "  demo                                              integrated run with PASS/FAIL summary\n"
        "  help                                              show this message\n", DEFAULT_QUANTUM);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);          /* keep cleanup running even if output is piped to head */
    if (argc < 2) { usage(stderr); return 2; }
    const char *c = argv[1];
    if (strcmp(c, "help") == 0 || strcmp(c, "-h") == 0 || strcmp(c, "--help") == 0) { usage(stdout); return 0; }
    if (strcmp(c, "sched") == 0)   return cmd_sched(argc, argv);
    if (strcmp(c, "logger") == 0)  return cmd_logger(argc, argv);
    if (strcmp(c, "ipc") == 0)     return argc == 2 ? run_ipc() : (fprintf(stderr, "usage: campuscompute ipc\n"), 2);
    if (strcmp(c, "banker") == 0)  return cmd_banker(argc, argv);
    if (strcmp(c, "worker") == 0)  return cmd_worker(argc, argv);
    if (strcmp(c, "rlimit") == 0)  return cmd_rlimit(argc, argv);
    if (strcmp(c, "cleanup") == 0) return cmd_cleanup();
    if (strcmp(c, "demo") == 0)    return cmd_demo();
    fprintf(stderr, "error: unknown command '%s'\n\n", c);
    usage(stderr);
    return 2;
}
