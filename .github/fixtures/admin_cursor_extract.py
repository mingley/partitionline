from pathlib import Path

def between(data, start, end):
    a=data.index(start)
    b=data.index(end,a)
    return data[a:b]
source=Path("src/admin.rs").read_text()
tests=Path("src/admin/pagination_tests.rs").read_text()
parts={
    "METHOD":between(source,"    async fn describe_topics_dtp<","\n    async fn describe_topics_metadata<"),
    "HISTORY":between(source,"// A fixed recent window and a Brent checkpoint","#[cfg(test)]\nstd::thread_local! {\n    static DTP_REMAINING_SCAN_PROGRESS"),
    "REPLACEMENT":between(tests,"#[tokio::test]\nasync fn dtp_pagination_rejects_nonadjacent_repeat_and_reuses_socket()","#[tokio::test]\nasync fn dtp_pagination_rejects_oversized_timeout_without_panicking()"),
    "APPENDED":tests[tests.index("#[test]\nfn dtp_cursor_history_detects_window_and_long_periods()"):],
}
for label, value in parts.items():
    print("CURSOR_BEGIN "+label)
    for line in value.splitlines(): print("CURSOR_LINE "+line)
    print("CURSOR_END "+label)
