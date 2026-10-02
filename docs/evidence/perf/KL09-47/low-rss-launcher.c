#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void print_memory(const char *label) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0)
        fprintf(stderr, "%s_RUSAGE_SELF_peak_bytes=%ld\n", label, usage.ru_maxrss * 1024L);
    FILE *file = fopen("/proc/self/status", "r");
    if (file) {
        char line[256]; long value;
        while (fgets(line, sizeof(line), file)) {
            if (sscanf(line, "VmHWM: %ld kB", &value) == 1)
                fprintf(stderr, "%s_current_executable_VmHWM_bytes=%ld\n", label, value * 1024L);
            if (sscanf(line, "VmRSS: %ld kB", &value) == 1)
                fprintf(stderr, "%s_current_RSS_bytes=%ld\n", label, value * 1024L);
        }
        fclose(file);
    }
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: low-rss-launcher PIDFILE EXECUTABLE [ARG...]\n");
        return 2;
    }
    print_memory("launcher_before_fork");
    fflush(NULL);
    pid_t child = fork();
    if (child < 0) { perror("fork"); return 3; }
    if (child == 0) {
        print_memory("child_after_fork_before_exec");
        fflush(NULL);
        execv(argv[2], &argv[2]);
        perror("execv");
        _exit(127);
    }
    FILE *pidfile = fopen(argv[1], "w");
    if (!pidfile) { perror("pidfile"); return 4; }
    fprintf(pidfile, "%ld\n", (long) child);
    if (fclose(pidfile)) { perror("pidfile close"); return 5; }
    int status;
    while (waitpid(child, &status, 0) < 0) {
        if (errno != EINTR) { perror("waitpid"); return 6; }
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return 7;
}
