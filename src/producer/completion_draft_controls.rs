//! Narrow controls for the experimental completion token.
//!
//! These controls do not exercise broker I/O, callback cleanup, transactions,
//! or shutdown. The associated production draft is not a completed repair.

use super::*;

fn pending(domain: &Arc<CompletionDomain>) -> Pending {
    let now = Instant::now();
    Pending {
        completion: None,
        completion_reservation: Some(domain.try_reserve().unwrap().unwrap()),
        sticky_record: None,
        sticky: None,
        rec: ProduceRecord::to("t").partition(0).value("value"),
        tx: None,
        seq: None,
        batch_base_seq: None,
        batch_len: 1,
        epoch_gen: 0,
        deadline: now + Duration::from_secs(30),
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    }
}

fn worker() -> (WorkerHandle, mpsc::Receiver<Pending>) {
    let (data, receiver) = mpsc::channel(1);
    let (ctrl, _control_receiver) = mpsc::channel(1);
    let admission = Arc::new(WorkerAdmission::new());
    let task = Arc::new(tokio::spawn(std::future::pending::<()>()));
    (
        WorkerHandle::new(Arc::new(()), admission, data, ctrl, task),
        receiver,
    )
}

#[tokio::test]
async fn worker_admission_holds_completion_until_terminal_publication() {
    let domain = CompletionDomain::new();
    let before = domain.cutoff();
    let (worker, mut receiver) = worker();
    assert!(worker.try_send(pending(&domain)).is_ok());
    let after = domain.cutoff();
    assert_eq!(domain.status(before), CompletionStatus::Complete);
    assert_eq!(domain.status(after), CompletionStatus::Pending);
    let mut pending = receiver.recv().await.unwrap();
    pending.complete_completion();
    assert_eq!(domain.status(after), CompletionStatus::Complete);
}

#[tokio::test]
async fn retry_handoff_keeps_the_original_completion_ticket() {
    let domain = CompletionDomain::new();
    let (first, mut first_rx) = worker();
    let (second, mut second_rx) = worker();
    assert!(first.try_send(pending(&domain)).is_ok());
    let cutoff = domain.cutoff();
    let pending = first_rx.recv().await.unwrap();
    first.admission.close();
    assert!(second.try_send(pending).is_ok());
    assert_eq!(domain.cutoff(), cutoff);
    assert_eq!(domain.status(cutoff), CompletionStatus::Pending);
    let mut retried = second_rx.recv().await.unwrap();
    retried.complete_completion();
    assert_eq!(domain.status(cutoff), CompletionStatus::Complete);
}

#[tokio::test]
async fn dropped_worker_owned_record_marks_completion_lost() {
    let domain = CompletionDomain::new();
    let (worker, mut receiver) = worker();
    assert!(worker.try_send(pending(&domain)).is_ok());
    let cutoff = domain.cutoff();
    drop(receiver.recv().await.unwrap());
    assert_eq!(domain.status(cutoff), CompletionStatus::Lost);
}
