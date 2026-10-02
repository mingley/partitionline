#include <stdio.h>
#include <sys/resource.h>
int main(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage)) return 1;
    printf("RUSAGE_SELF_peak_bytes=%ld\n", usage.ru_maxrss * 1024L);
    FILE *file = fopen("/proc/self/status", "r");
    if (!file) return 2;
    char line[256];
    while (fgets(line, sizeof(line), file)) {
        long value;
        if (sscanf(line, "VmHWM: %ld kB", &value) == 1)
            printf("current_executable_VmHWM_bytes=%ld\n", value * 1024L);
    }
    return fclose(file) ? 3 : 0;
}
