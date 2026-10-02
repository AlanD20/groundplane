#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/capability.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <openssl/evp.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <grp.h>

#if defined(__x86_64__)
#define GP_AUDIT_ARCH AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define GP_AUDIT_ARCH AUDIT_ARCH_AARCH64
#else
#error Unsupported PostgreSQL helper gate architecture
#endif

enum { FD_INTENT = 3, FD_STATUS = 4, FD_RELEASE = 5, FD_CLIENT = 6 };
enum { STATUS_READY = 1, STATUS_PROFILE = 2, STATUS_FATAL = 3 };
enum { STAGE_SET_PROCESS_GROUP = 1, STAGE_SET_LIMIT = 2,
       STAGE_INITIAL_PARENT_DEATH = 3, STAGE_INITIAL_PARENT_IDENTITY = 4,
       STAGE_INTENT_REBUILD = 5, STAGE_READY_FRAME = 6,
       STAGE_RELEASE_READ = 7, STAGE_POST_RELEASE_PARENT = 8,
       STAGE_LAST_CAPABILITY = 9, STAGE_BOUNDING_DROP = 10,
       STAGE_GROUPS = 11, STAGE_GIDS = 12, STAGE_UIDS = 13,
       STAGE_CAPABILITY_CLEAR = 14, STAGE_NO_NEW_PRIVILEGES = 15,
       STAGE_PARENT_DEATH_REARM = 16, STAGE_FINAL_PARENT_IDENTITY = 17,
       STAGE_CLIENT_FILE = 18, STAGE_SECCOMP = 19,
       STAGE_CLOSE_RANGE_OR_FD_SET = 20, STAGE_PROFILE_FRAME = 21,
       STAGE_EXEC = 22 };

static const char *const exact_environment[] = {
    "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "HOME=/nonexistent", "LC_ALL=C", "TZ=UTC", NULL
};

struct capsule {
    unsigned char nonce[32];
    unsigned char intent_sha[32];
    uint32_t parent_pid;
    uint64_t parent_start;
    unsigned char boot_id[16];
    uint64_t deadline_ns;
    uint64_t gate_dev;
    uint64_t gate_ino;
    uint64_t client_dev;
    uint64_t client_ino;
    uint64_t client_size;
    unsigned char client_sha[32];
    unsigned char operation;
    unsigned char argc;
    char *argv[33];
    char arg_storage[16416];
    unsigned char data[32768];
};

static uint16_t get16(const unsigned char *p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}
static uint32_t get32(const unsigned char *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | p[3];
}
static uint64_t get64(const unsigned char *p) {
    return ((uint64_t)get32(p) << 32) | get32(p + 4);
}
static void put32(unsigned char *p, uint32_t value) {
    p[0] = (unsigned char)(value >> 24);
    p[1] = (unsigned char)(value >> 16);
    p[2] = (unsigned char)(value >> 8);
    p[3] = (unsigned char)value;
}

static int write_all(int fd, const void *data, size_t size) {
    const unsigned char *p = data;
    while (size != 0) {
        ssize_t n = write(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

/* Fixed 79-byte status. The supervisor supplies independently observed kernel
 * identity to the shared ConfinementGateStatus validator. */
static int emit_status(const struct capsule *c, unsigned char kind,
                       unsigned char stage, uint32_t error_number) {
    unsigned char frame[79];
    memcpy(frame, "GPPG16S1", 8);
    frame[8] = kind == STATUS_READY ? 1 : kind == STATUS_PROFILE ? 2 :
               stage <= STAGE_READY_FRAME ? 1 : stage <= STAGE_PROFILE_FRAME ? 2 : 3;
    frame[9] = kind;
    frame[10] = stage;
    put32(frame + 11, error_number);
    memcpy(frame + 15, c->nonce, 32);
    memcpy(frame + 47, c->intent_sha, 32);
    return write_all(FD_STATUS, frame, sizeof(frame));
}

static void fatal(const struct capsule *c, unsigned char stage, int error_number) {
    if (error_number <= 0) error_number = EIO;
    (void)emit_status(c, STATUS_FATAL, stage, (uint32_t)error_number);
    _exit(4);
}

static int read_capsule(struct capsule *c) {
    int seals = fcntl(FD_INTENT, F_GET_SEALS);
    int required = F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
    if (seals < 0 || (seals & required) != required) return -1;
    struct stat st;
    if (fstat(FD_INTENT, &st) != 0 || st.st_size < 186 ||
        st.st_size > (off_t)sizeof(c->data)) return -1;
    size_t size = (size_t)st.st_size;
    size_t offset = 0;
    while (offset < size) {
        ssize_t n = pread(FD_INTENT, c->data + offset, size - offset, (off_t)offset);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        offset += (size_t)n;
    }
    const unsigned char *p = c->data;
    if (memcmp(p, "GPPG16G1", 8) != 0 || get32(p + 8) != 1) return -1;
    memcpy(c->nonce, p + 12, 32);
    memcpy(c->intent_sha, p + 44, 32);
    c->parent_pid = get32(p + 76);
    c->parent_start = get64(p + 80);
    memcpy(c->boot_id, p + 88, 16);
    c->deadline_ns = get64(p + 104);
    c->gate_dev = get64(p + 112);
    c->gate_ino = get64(p + 120);
    c->client_dev = get64(p + 128);
    c->client_ino = get64(p + 136);
    c->client_size = get64(p + 144);
    memcpy(c->client_sha, p + 152, 32);
    c->operation = p[184];
    c->argc = p[185];
    if (c->parent_pid == 0 || c->parent_start == 0 || c->deadline_ns == 0 ||
        c->operation < 1 || c->operation > 10 || c->argc == 0 || c->argc > 32) return -1;
    offset = 186;
    size_t stored = 0;
    for (unsigned int i = 0; i < c->argc; i++) {
        if (offset + 2 > size) return -1;
        uint16_t length = get16(p + offset);
        offset += 2;
        if (length == 0 || length > 4096 || offset + length > size ||
            memchr(p + offset, 0, length) != NULL) return -1;
        if (stored + length + 1 > sizeof(c->arg_storage)) return -1;
        c->argv[i] = c->arg_storage + stored;
        memcpy(c->argv[i], p + offset, length);
        c->argv[i][length] = 0;
        stored += length + 1;
        offset += length;
    }
    if (offset != size) return -1;
    c->argv[c->argc] = NULL;
    return 0;
}

static int exact_env(void) {
    extern char **environ;
    for (unsigned int i = 0; i < 4; i++) {
        if (environ[i] == NULL || strcmp(environ[i], exact_environment[i]) != 0) return -1;
    }
    return environ[4] == NULL ? 0 : -1;
}

static int exact_fds(int profile) {
    for (int fd = 0; fd < 64; fd++) {
        int wanted = fd <= 6 && (fd <= 2 || fd == FD_STATUS || fd == FD_CLIENT ||
                     fd == FD_RELEASE || (!profile && fd == FD_INTENT));
        int present = fcntl(fd, F_GETFD) >= 0;
        if (present != wanted) return -1;
    }
    return 0;
}

static int boot_matches(const unsigned char expected[16]) {
    int fd = open("/proc/sys/kernel/random/boot_id", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    char raw[64];
    ssize_t n = read(fd, raw, sizeof(raw));
    close(fd);
    if (n < 36 || n > (ssize_t)sizeof(raw)) return -1;
    unsigned char decoded[16];
    unsigned int count = 0;
    for (ssize_t i = 0; i < n && count < 32; i++) {
        if (raw[i] == '-') continue;
        int value = raw[i] >= '0' && raw[i] <= '9' ? raw[i] - '0' :
                    raw[i] >= 'a' && raw[i] <= 'f' ? raw[i] - 'a' + 10 : -1;
        if (value < 0) return -1;
        if ((count & 1U) == 0) decoded[count / 2] = (unsigned char)(value << 4);
        else decoded[count / 2] |= (unsigned char)value;
        count++;
    }
    return count == 32 && memcmp(decoded, expected, 16) == 0 ? 0 : -1;
}

static int parent_matches(const struct capsule *c) {
    if (getppid() != (pid_t)c->parent_pid || boot_matches(c->boot_id) != 0) return -1;
    int pidfd = (int)syscall(SYS_pidfd_open, (pid_t)c->parent_pid, 0);
    if (pidfd < 0) return -1;
    char path[64];
    int path_length = snprintf(path, sizeof(path), "/proc/%u/stat", c->parent_pid);
    if (path_length <= 0 || (size_t)path_length >= sizeof(path)) { close(pidfd); return -1; }
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) { close(pidfd); return -1; }
    char statbuf[4096];
    ssize_t n = read(fd, statbuf, sizeof(statbuf) - 1);
    close(fd);
    if (n <= 0 || n >= (ssize_t)sizeof(statbuf)) { close(pidfd); return -1; }
    statbuf[n] = 0;
    char *end = strrchr(statbuf, ')');
    if (end == NULL || end[1] != ' ') { close(pidfd); return -1; }
    char *cursor = end + 2;
    uint64_t start = 0;
    for (unsigned int field = 3; field <= 22; field++) {
        char *next = strchr(cursor, ' ');
        if (field < 22 && next == NULL) { close(pidfd); return -1; }
        if (field == 22) {
            errno = 0;
            start = strtoull(cursor, NULL, 10);
            if (errno != 0) { close(pidfd); return -1; }
        } else cursor = next + 1;
    }
    struct pollfd observed = { pidfd, POLLIN, 0 };
    int alive = poll(&observed, 1, 0) == 0;
    close(pidfd);
    return alive && start == c->parent_start && getppid() == (pid_t)c->parent_pid ? 0 : -1;
}

static int digest_client(const struct capsule *c) {
    struct stat st;
    if (fstat(FD_CLIENT, &st) != 0 || !S_ISREG(st.st_mode) ||
        (uint64_t)st.st_dev != c->client_dev || (uint64_t)st.st_ino != c->client_ino ||
        (uint64_t)st.st_size != c->client_size || st.st_uid != 0 || st.st_gid != 0 ||
        (st.st_mode & 06022) != 0) return -1;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(ctx); return -1;
    }
    unsigned char buffer[32768];
    uint64_t offset = 0;
    while (offset < c->client_size) {
        size_t wanted = sizeof(buffer);
        if (c->client_size - offset < wanted) wanted = (size_t)(c->client_size - offset);
        ssize_t n = pread(FD_CLIENT, buffer, wanted, (off_t)offset);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0 || EVP_DigestUpdate(ctx, buffer, (size_t)n) != 1) {
            EVP_MD_CTX_free(ctx); return -1;
        }
        offset += (uint64_t)n;
    }
    unsigned char digest[32];
    unsigned int length = 0;
    int okay = EVP_DigestFinal_ex(ctx, digest, &length) == 1;
    EVP_MD_CTX_free(ctx);
    struct stat after;
    return okay && length == 32 && memcmp(digest, c->client_sha, 32) == 0 &&
           fstat(FD_CLIENT, &after) == 0 && after.st_dev == st.st_dev &&
           after.st_ino == st.st_ino && after.st_size == st.st_size &&
           after.st_mtim.tv_sec == st.st_mtim.tv_sec &&
           after.st_mtim.tv_nsec == st.st_mtim.tv_nsec ? 0 : -1;
}

static int drop_privileges(void) {
    int fd = open("/proc/sys/kernel/cap_last_cap", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    char raw[32];
    ssize_t n = read(fd, raw, sizeof(raw) - 1);
    close(fd);
    if (n <= 0 || n >= (ssize_t)sizeof(raw)) return -1;
    raw[n] = 0;
    char *end = NULL;
    long last = strtol(raw, &end, 10);
    if (end == raw || last < 0 || last > 63) return -1;
    for (long cap = 0; cap <= last; cap++) {
        if (prctl(PR_CAPBSET_DROP, cap, 0, 0, 0) != 0) return -1;
    }
    if (setgroups(0, NULL) != 0) return -2;
    if (setresgid(70, 70, 70) != 0) return -3;
    if (setresuid(70, 70, 70) != 0) return -4;
    struct __user_cap_header_struct header = { _LINUX_CAPABILITY_VERSION_3, 0 };
    struct __user_cap_data_struct data[2] = {{0}};
    if (syscall(SYS_capset, &header, data) != 0) return -5;
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) return -6;
    return 0;
}

static int apply_seccomp(void) {
    struct sock_filter filter[] = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, GP_AUDIT_ARCH, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone3, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
#ifdef SYS_fork
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_fork, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
#endif
#ifdef SYS_vfork
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_vfork, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
#endif
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog program = { (unsigned short)(sizeof(filter) / sizeof(filter[0])), filter };
    return prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &program, 0, 0);
}

int main(int argc, char **argv) {
    (void)argv;
    struct capsule c = {0};
    if (argc != 1 || exact_env() != 0 || getuid() != 0 || geteuid() != 0 ||
        getgid() != 0 || getegid() != 0 || read_capsule(&c) != 0) _exit(4);
    if (setpgid(0, 0) != 0) fatal(&c, STAGE_SET_PROCESS_GROUP, errno);
    struct rlimit limit = {64, 64};
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0) fatal(&c, STAGE_SET_LIMIT, errno);
    if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) != 0)
        fatal(&c, STAGE_INITIAL_PARENT_DEATH, errno);
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 ||
        fcntl(FD_STATUS, F_SETFD, FD_CLOEXEC) != 0 ||
        fcntl(FD_CLIENT, F_SETFD, FD_CLOEXEC) != 0)
        fatal(&c, STAGE_INTENT_REBUILD, errno);
    if (parent_matches(&c) != 0) fatal(&c, STAGE_INITIAL_PARENT_IDENTITY, ESRCH);
    struct stat own;
    if (stat("/proc/self/exe", &own) != 0 || (uint64_t)own.st_dev != c.gate_dev ||
        (uint64_t)own.st_ino != c.gate_ino || exact_fds(0) != 0)
        fatal(&c, STAGE_INTENT_REBUILD, EINVAL);
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0 ||
        (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec >= c.deadline_ns)
        fatal(&c, STAGE_INTENT_REBUILD, ETIMEDOUT);
    if (digest_client(&c) != 0) fatal(&c, STAGE_CLIENT_FILE, EINVAL);
    if (emit_status(&c, STATUS_READY, 0, 0) != 0) fatal(&c, STAGE_READY_FRAME, errno);
    unsigned char release = 0;
    if (read(FD_RELEASE, &release, 1) != 1 || release != 0xa5)
        fatal(&c, STAGE_RELEASE_READ, EINVAL);
    if (parent_matches(&c) != 0) fatal(&c, STAGE_POST_RELEASE_PARENT, ESRCH);
    int dropped = drop_privileges();
    if (dropped != 0) {
        unsigned char stage = dropped == -1 ? STAGE_BOUNDING_DROP :
            dropped == -2 ? STAGE_GROUPS : dropped == -3 ? STAGE_GIDS :
            dropped == -4 ? STAGE_UIDS : dropped == -5 ? STAGE_CAPABILITY_CLEAR :
            STAGE_NO_NEW_PRIVILEGES;
        fatal(&c, stage, errno);
    }
    if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) != 0)
        fatal(&c, STAGE_PARENT_DEATH_REARM, errno);
    if (parent_matches(&c) != 0) fatal(&c, STAGE_FINAL_PARENT_IDENTITY, ESRCH);
    if (digest_client(&c) != 0) fatal(&c, STAGE_CLIENT_FILE, EINVAL);
    if (apply_seccomp() != 0) fatal(&c, STAGE_SECCOMP, errno);
    close(FD_INTENT);
    for (int fd = 7; fd < 64; fd++) close(fd);
    if (exact_fds(1) != 0) fatal(&c, STAGE_CLOSE_RANGE_OR_FD_SET, EINVAL);
    if (emit_status(&c, STATUS_PROFILE, 0, 0) != 0)
        fatal(&c, STAGE_PROFILE_FRAME, errno);
    release = 0;
    if (read(FD_RELEASE, &release, 1) != 1 || release != 0x5a)
        fatal(&c, STAGE_RELEASE_READ, EINVAL);
    close(FD_RELEASE);
    syscall(SYS_execveat, FD_CLIENT, "", c.argv, exact_environment, AT_EMPTY_PATH);
    fatal(&c, STAGE_EXEC, errno);
}
