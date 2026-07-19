use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore},
    time::{self, Instant},
};

use crate::{OutboundFrame, OutboundSink, SinkWriteFuture, SinkWriteOutcome};

const CLOSED_ERROR: &str = "transport writer is closed";
const DROPPED_ERROR: &str = "transport writer dropped before reporting a write result";
const MATERIALIZED_TWICE_ERROR: &str = "deferred outbound payload was materialized more than once";

type DeferredWireEncoder = Box<dyn FnOnce() -> Result<Vec<u8>, String> + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriterPhase {
    Staging,
    Started,
    Closed,
}

struct WriterStateData {
    phase: WriterPhase,
    bootstrap: VecDeque<QueuedWrite>,
}

struct WriterState {
    data: Mutex<WriterStateData>,
    changed: Notify,
}

impl WriterState {
    fn new() -> Self {
        Self {
            data: Mutex::new(WriterStateData {
                phase: WriterPhase::Staging,
                bootstrap: VecDeque::new(),
            }),
            changed: Notify::new(),
        }
    }

    fn is_closed(&self) -> bool {
        self.data
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phase
            == WriterPhase::Closed
    }

    fn stage_bootstrap(&self, request: QueuedWrite) -> Result<(), QueuedWrite> {
        let mut data = self.data.lock().unwrap_or_else(|error| error.into_inner());
        if data.phase != WriterPhase::Staging {
            return Err(request);
        }
        data.bootstrap.push_back(request);
        Ok(())
    }

    fn start(&self) -> bool {
        let started = {
            let mut data = self.data.lock().unwrap_or_else(|error| error.into_inner());
            if data.phase != WriterPhase::Staging {
                false
            } else {
                data.phase = WriterPhase::Started;
                true
            }
        };
        if started {
            self.changed.notify_waiters();
        }
        started
    }

    fn close(&self) {
        let pending_bootstrap = {
            let mut data = self.data.lock().unwrap_or_else(|error| error.into_inner());
            if data.phase == WriterPhase::Closed {
                return;
            }
            data.phase = WriterPhase::Closed;
            std::mem::take(&mut data.bootstrap)
        };
        for request in pending_bootstrap {
            request.complete(definitely_not_written(CLOSED_ERROR));
        }
        self.changed.notify_waiters();
    }

    fn pop_bootstrap_or_phase(&self) -> (Option<QueuedWrite>, WriterPhase) {
        let mut data = self.data.lock().unwrap_or_else(|error| error.into_inner());
        (data.bootstrap.pop_front(), data.phase)
    }

    async fn wait_until_started_or_closed(&self) -> WriterPhase {
        loop {
            let changed = self.changed.notified();
            let phase = self
                .data
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .phase;
            if phase != WriterPhase::Staging {
                return phase;
            }
            changed.await;
        }
    }

    async fn closed(&self) {
        loop {
            let changed = self.changed.notified();
            if self.is_closed() {
                return;
            }
            changed.await;
        }
    }
}

/// A bounded outbound sink whose completion reflects the actual writer result.
///
/// This is an internal transport primitive, not part of the public gateway API.
#[doc(hidden)]
#[derive(Clone)]
pub(crate) struct QueuedOutboundSink {
    sender: mpsc::Sender<QueuedWrite>,
    state: Arc<WriterState>,
    queue_capacity: usize,
    admission: Arc<Semaphore>,
}

impl QueuedOutboundSink {
    pub(crate) fn new(
        queue_capacity: usize,
        sequence_gap_timeout: Duration,
    ) -> (Arc<Self>, QueuedWriter) {
        assert!(queue_capacity > 0, "writer queue capacity must be positive");
        assert!(
            !sequence_gap_timeout.is_zero(),
            "sequence gap timeout must be positive"
        );
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let state = Arc::new(WriterState::new());
        let admission = Arc::new(Semaphore::new(queue_capacity));
        (
            Arc::new(Self {
                sender,
                state: Arc::clone(&state),
                queue_capacity,
                admission: Arc::clone(&admission),
            }),
            QueuedWriter {
                receiver,
                state,
                closed: false,
                next_sequence: 1,
                reordered: BTreeMap::new(),
                reorder_capacity: queue_capacity,
                sequence_gap_timeout,
                gap_started: None,
            },
        )
    }

    /// Stages handshake bytes ahead of every normal outbound frame.
    ///
    /// The returned receiver resolves only after the writer reports the actual
    /// transport result. Staging after `start_writer` fails closed.
    #[cfg(test)]
    pub(crate) fn stage_bootstrap(
        &self,
        wire_bytes: Vec<u8>,
    ) -> oneshot::Receiver<SinkWriteOutcome> {
        self.stage_bootstrap_payload(QueuedPayload::Ready(wire_bytes))
    }

    pub(crate) fn stage_bootstrap_deferred<F>(
        &self,
        encoder: F,
    ) -> oneshot::Receiver<SinkWriteOutcome>
    where
        F: FnOnce() -> Result<Vec<u8>, String> + Send + 'static,
    {
        self.stage_bootstrap_payload(QueuedPayload::Deferred(Box::new(encoder)))
    }

    fn stage_bootstrap_payload(
        &self,
        payload: QueuedPayload,
    ) -> oneshot::Receiver<SinkWriteOutcome> {
        let (request, completion) = QueuedWrite::new(1, payload, None, false);
        match self.state.stage_bootstrap(request) {
            Ok(()) => completion,
            Err(request) => {
                request.complete(definitely_not_written(
                    "bootstrap must be staged before the transport writer starts",
                ));
                completion
            }
        }
    }

    /// Opens the writer gate after all bootstrap messages have been staged.
    pub(crate) fn start_writer(&self) -> bool {
        self.state.start()
    }

    pub(crate) fn close(&self) {
        self.state.close();
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Waits until replacement fencing, disconnect, or writer failure closes
    /// this transport binding.
    pub(crate) async fn closed(&self) {
        self.state.closed().await;
    }

    fn enqueue(&self, frame: OutboundFrame) -> SinkWriteFuture<'static> {
        self.enqueue_payload(frame.sequence, QueuedPayload::Ready(frame.wire_bytes))
    }

    /// Queues an outbound sequence while deferring timestamp sampling and JSON
    /// encoding until the concrete transport writer is ready to write it.
    pub(crate) fn enqueue_deferred<F>(&self, sequence: u64, encoder: F) -> SinkWriteFuture<'static>
    where
        F: FnOnce() -> Result<Vec<u8>, String> + Send + 'static,
    {
        self.enqueue_payload(sequence, QueuedPayload::Deferred(Box::new(encoder)))
    }

    fn enqueue_payload(&self, sequence: u64, payload: QueuedPayload) -> SinkWriteFuture<'static> {
        if self.state.is_closed() {
            return ready_outcome(definitely_not_written(CLOSED_ERROR));
        }

        let permit = match Arc::clone(&self.admission).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.state.close();
                return ready_outcome(SinkWriteOutcome::Backpressure {
                    queue_depth: self
                        .queue_capacity
                        .saturating_sub(self.admission.available_permits()),
                });
            }
        };
        let (request, completion) = QueuedWrite::new(sequence, payload, Some(permit), false);
        match self.sender.try_send(request) {
            Ok(()) => completion_future(completion),
            Err(mpsc::error::TrySendError::Full(request)) => {
                request.complete(SinkWriteOutcome::Backpressure {
                    queue_depth: self.queue_capacity,
                });
                // This sequence was durably reserved but cannot reach the
                // writer. Later messages must not overtake the resulting gap.
                self.state.close();
                completion_future(completion)
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                self.state.close();
                request.complete(definitely_not_written(CLOSED_ERROR));
                completion_future(completion)
            }
        }
    }

    fn enqueue_skip(&self, sequence: u64) {
        if self.state.is_closed() {
            return;
        }
        let Ok(permit) = Arc::clone(&self.admission).try_acquire_owned() else {
            self.state.close();
            return;
        };
        let (request, _completion) = QueuedWrite::new(
            sequence,
            QueuedPayload::Ready(Vec::new()),
            Some(permit),
            true,
        );
        if self.sender.try_send(request).is_err() {
            self.state.close();
        }
    }
}

pub(crate) struct QueuedSinkDropGuard(Arc<QueuedOutboundSink>);

impl QueuedSinkDropGuard {
    pub(crate) fn new(sink: Arc<QueuedOutboundSink>) -> Self {
        Self(sink)
    }
}

impl Drop for QueuedSinkDropGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl OutboundSink for QueuedOutboundSink {
    fn write<'a>(&'a self, frame: OutboundFrame) -> SinkWriteFuture<'a> {
        self.enqueue(frame)
    }

    fn skip(&self, sequence: u64) {
        self.enqueue_skip(sequence);
    }

    fn close(&self) {
        QueuedOutboundSink::close(self);
    }
}

/// The sole consumer allowed to perform writes on one transport connection.
#[doc(hidden)]
pub(crate) struct QueuedWriter {
    receiver: mpsc::Receiver<QueuedWrite>,
    state: Arc<WriterState>,
    closed: bool,
    next_sequence: u64,
    reordered: BTreeMap<u64, QueuedWrite>,
    reorder_capacity: usize,
    sequence_gap_timeout: Duration,
    gap_started: Option<Instant>,
}

impl QueuedWriter {
    pub(crate) fn closed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let state = Arc::clone(&self.state);
        async move { state.closed().await }
    }

    pub(crate) fn fail_closed(&mut self, error: &str) {
        self.state.close();
        self.fail_pending(error);
    }

    /// Returns bootstrap writes first, then ordinary bounded-queue writes.
    /// This method waits while the writer startup gate remains closed.
    pub(crate) async fn next(&mut self) -> Option<QueuedWrite> {
        if self.closed {
            return None;
        }

        if self.state.wait_until_started_or_closed().await == WriterPhase::Closed {
            self.fail_pending(CLOSED_ERROR);
            return None;
        }

        loop {
            if !self.reordered.is_empty() && self.gap_started.is_none() {
                self.gap_started = Some(Instant::now());
            }
            let (bootstrap, phase) = self.state.pop_bootstrap_or_phase();
            if phase == WriterPhase::Closed {
                self.fail_pending(CLOSED_ERROR);
                return None;
            }
            if let Some(request) = bootstrap {
                match self.accept_expected(request) {
                    Some(request) if request.skip => continue,
                    result => return result,
                }
            }
            if let Some(request) = self.reordered.remove(&self.next_sequence) {
                match self.accept_expected(request) {
                    Some(request) if request.skip => continue,
                    result => return result,
                }
            }

            let state = Arc::clone(&self.state);
            let request = tokio::select! {
                biased;
                () = state.closed() => {
                    self.fail_pending(CLOSED_ERROR);
                    return None;
                }
                () = wait_for_gap(self.gap_started, self.sequence_gap_timeout) => {
                    self.state.close();
                    self.fail_pending("outbound sequence gap timed out");
                    return None;
                }
                request = self.receiver.recv() => request,
            };
            let Some(request) = request else {
                self.state.close();
                self.fail_pending(CLOSED_ERROR);
                return None;
            };
            if request.sequence == self.next_sequence {
                match self.accept_expected(request) {
                    Some(request) if request.skip => continue,
                    result => return result,
                }
            }
            if request.sequence < self.next_sequence
                || self.reordered.contains_key(&request.sequence)
                || self.reordered.len() >= self.reorder_capacity
            {
                request.complete(definitely_not_written(
                    "outbound sequence is duplicated, stale, or exceeds the reorder bound",
                ));
                self.state.close();
                self.fail_pending(CLOSED_ERROR);
                return None;
            }
            self.reordered.insert(request.sequence, request);
        }
    }

    fn accept_expected(&mut self, request: QueuedWrite) -> Option<QueuedWrite> {
        if request.sequence != self.next_sequence {
            request.complete(definitely_not_written(
                "bootstrap or outbound sequence does not match the writer high-water mark",
            ));
            self.state.close();
            self.fail_pending(CLOSED_ERROR);
            return None;
        }
        self.next_sequence = match self.next_sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                request.complete(definitely_not_written("outbound sequence overflow"));
                self.state.close();
                self.fail_pending(CLOSED_ERROR);
                return None;
            }
        };
        self.gap_started = None;
        Some(request)
    }

    fn fail_pending(&mut self, error: &str) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.receiver.close();
        for (_, request) in std::mem::take(&mut self.reordered) {
            request.complete(definitely_not_written(error));
        }
        while let Ok(request) = self.receiver.try_recv() {
            request.complete(definitely_not_written(error));
        }
    }
}

impl Drop for QueuedWriter {
    fn drop(&mut self) {
        self.state.close();
        self.fail_pending(DROPPED_ERROR);
    }
}

/// One write owned exclusively by `QueuedWriter` until it reports an outcome.
#[doc(hidden)]
pub(crate) struct QueuedWrite {
    sequence: u64,
    payload: QueuedPayload,
    skip: bool,
    _permit: Option<OwnedSemaphorePermit>,
    completion: Option<oneshot::Sender<SinkWriteOutcome>>,
}

enum QueuedPayload {
    Ready(Vec<u8>),
    Deferred(DeferredWireEncoder),
    Materializing,
}

impl QueuedWrite {
    fn new(
        sequence: u64,
        payload: QueuedPayload,
        permit: Option<OwnedSemaphorePermit>,
        skip: bool,
    ) -> (Self, oneshot::Receiver<SinkWriteOutcome>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sequence,
                payload,
                skip,
                _permit: permit,
                completion: Some(sender),
            },
            receiver,
        )
    }

    /// Materializes deferred bytes at the concrete transport write boundary.
    ///
    /// The encoder is invoked at most once. A failure leaves the request in a
    /// terminal state so the owning transport writer can fail the connection
    /// closed without risking a second timestamp sample or alternate payload.
    pub(crate) fn materialize_wire_bytes(&mut self) -> Result<&[u8], String> {
        if matches!(self.payload, QueuedPayload::Deferred(_)) {
            let payload = std::mem::replace(&mut self.payload, QueuedPayload::Materializing);
            let QueuedPayload::Deferred(encoder) = payload else {
                unreachable!("deferred payload changed while exclusively borrowed");
            };
            self.payload = match encoder() {
                Ok(wire_bytes) => QueuedPayload::Ready(wire_bytes),
                Err(error) => return Err(error),
            };
        }
        match &self.payload {
            QueuedPayload::Ready(wire_bytes) => Ok(wire_bytes),
            QueuedPayload::Deferred(_) => unreachable!("deferred payload was not materialized"),
            QueuedPayload::Materializing => Err(MATERIALIZED_TWICE_ERROR.to_owned()),
        }
    }

    #[cfg(test)]
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Resolves the sink write with evidence produced by the actual transport
    /// operation. Queue admission alone never calls this method.
    pub(crate) fn complete(mut self, outcome: SinkWriteOutcome) {
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(outcome);
        }
    }
}

impl Drop for QueuedWrite {
    fn drop(&mut self) {
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(definitely_not_written(DROPPED_ERROR));
        }
    }
}

fn definitely_not_written(error: &str) -> SinkWriteOutcome {
    SinkWriteOutcome::DefinitelyNotWritten {
        error: error.to_owned(),
    }
}

fn ready_outcome(outcome: SinkWriteOutcome) -> SinkWriteFuture<'static> {
    Box::pin(std::future::ready(outcome))
}

fn completion_future(completion: oneshot::Receiver<SinkWriteOutcome>) -> SinkWriteFuture<'static> {
    Box::pin(async move {
        completion
            .await
            .unwrap_or_else(|_| definitely_not_written(DROPPED_ERROR))
    })
}

async fn wait_for_gap(started: Option<Instant>, timeout: Duration) {
    match started {
        Some(started) => time::sleep_until(started + timeout).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sinan_types::{MessageId, SessionId};

    use super::*;

    fn frame(sequence: u64, bytes: &[u8]) -> OutboundFrame {
        OutboundFrame {
            session_id: SessionId::from("session_1"),
            message_id: MessageId::from(format!("message_{sequence}")),
            sequence,
            wire_bytes: bytes.to_vec(),
        }
    }

    async fn started_writer(capacity: usize) -> (Arc<QueuedOutboundSink>, QueuedWriter) {
        let (sink, mut writer) = QueuedOutboundSink::new(capacity, Duration::from_secs(1));
        let bootstrap = sink.stage_bootstrap(b"accepted".to_vec());
        assert!(sink.start_writer());
        let request = writer.next().await.unwrap();
        assert_eq!(request.sequence(), 1);
        request.complete(SinkWriteOutcome::Written);
        assert_eq!(bootstrap.await.unwrap(), SinkWriteOutcome::Written);
        (sink, writer)
    }

    #[tokio::test]
    async fn bootstrap_precedes_frames_queued_before_writer_start() {
        let (sink, mut writer) = QueuedOutboundSink::new(2, Duration::from_secs(1));
        let bootstrap = sink.stage_bootstrap(b"accepted".to_vec());
        let ordinary = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(2, b"command")).await }
        });
        tokio::task::yield_now().await;

        assert!(sink.start_writer());
        let mut first = writer.next().await.unwrap();
        assert_eq!(first.materialize_wire_bytes().unwrap(), b"accepted");
        first.complete(SinkWriteOutcome::Written);
        assert_eq!(bootstrap.await.unwrap(), SinkWriteOutcome::Written);

        let mut second = writer.next().await.unwrap();
        assert_eq!(second.materialize_wire_bytes().unwrap(), b"command");
        second.complete(SinkWriteOutcome::Written);
        assert_eq!(ordinary.await.unwrap(), SinkWriteOutcome::Written);
    }

    #[tokio::test]
    async fn full_queue_reports_the_real_depth_without_admitting_bytes() {
        let (sink, mut writer) = started_writer(1).await;
        let first = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(2, b"first")).await }
        });
        tokio::task::yield_now().await;

        assert_eq!(
            sink.write(frame(3, b"second")).await,
            SinkWriteOutcome::Backpressure { queue_depth: 1 }
        );
        assert!(writer.next().await.is_none());
        assert!(matches!(
            first.await.unwrap(),
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[tokio::test]
    async fn queue_admission_does_not_complete_the_sink_write() {
        let (sink, mut writer) = started_writer(1).await;
        let write = tokio::spawn(async move { sink.write(frame(2, b"command")).await });

        let request = writer.next().await.unwrap();
        assert!(!write.is_finished());
        request.complete(SinkWriteOutcome::Unconfirmed {
            error: "socket closed during write".to_owned(),
        });

        assert!(matches!(
            write.await.unwrap(),
            SinkWriteOutcome::Unconfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn close_wakes_observers_and_fails_queued_and_future_writes() {
        let (sink, mut writer) = started_writer(2).await;
        let pending = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(2, b"pending")).await }
        });
        tokio::task::yield_now().await;

        OutboundSink::close(sink.as_ref());
        sink.closed().await;
        assert!(sink.is_closed());
        assert!(writer.next().await.is_none());
        assert!(matches!(
            pending.await.unwrap(),
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
        assert!(matches!(
            sink.write(frame(3, b"late")).await,
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[tokio::test]
    async fn dropping_writer_resolves_every_pending_completion() {
        let (sink, writer) = started_writer(2).await;
        let first = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(2, b"first")).await }
        });
        let second = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(3, b"second")).await }
        });
        tokio::task::yield_now().await;

        drop(writer);

        assert!(matches!(
            first.await.unwrap(),
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
        assert!(matches!(
            second.await.unwrap(),
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[tokio::test]
    async fn writer_orders_concurrent_enqueues_by_reserved_sequence() {
        let (sink, mut writer) = started_writer(4).await;
        let third = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(3, b"third")).await }
        });
        tokio::task::yield_now().await;
        let second = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(2, b"second")).await }
        });

        let first_request = writer.next().await.unwrap();
        assert_eq!(first_request.sequence(), 2);
        first_request.complete(SinkWriteOutcome::Written);
        let second_request = writer.next().await.unwrap();
        assert_eq!(second_request.sequence(), 3);
        second_request.complete(SinkWriteOutcome::Written);

        assert_eq!(second.await.unwrap(), SinkWriteOutcome::Written);
        assert_eq!(third.await.unwrap(), SinkWriteOutcome::Written);
    }

    #[tokio::test]
    async fn explicit_skip_advances_an_intentionally_empty_sequence() {
        let (sink, mut writer) = started_writer(2).await;
        OutboundSink::skip(sink.as_ref(), 2);
        let third = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(3, b"third")).await }
        });

        let request = writer.next().await.unwrap();
        assert_eq!(request.sequence(), 3);
        request.complete(SinkWriteOutcome::Written);
        assert_eq!(third.await.unwrap(), SinkWriteOutcome::Written);
    }

    #[tokio::test]
    async fn unresolved_sequence_gap_closes_the_writer_within_the_bound() {
        let (sink, mut writer) = QueuedOutboundSink::new(2, Duration::from_millis(20));
        let bootstrap = sink.stage_bootstrap(b"accepted".to_vec());
        assert!(sink.start_writer());
        writer
            .next()
            .await
            .unwrap()
            .complete(SinkWriteOutcome::Written);
        assert_eq!(bootstrap.await.unwrap(), SinkWriteOutcome::Written);
        let third = tokio::spawn({
            let sink = sink.clone();
            async move { sink.write(frame(3, b"third")).await }
        });

        assert!(writer.next().await.is_none());
        assert!(matches!(
            third.await.unwrap(),
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[tokio::test]
    async fn deferred_payload_is_materialized_only_at_the_ordered_write_boundary() {
        let (sink, mut writer) = started_writer(3).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let deferred = sink.enqueue_deferred(3, {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(b"sampled-at-write".to_vec())
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let second = sink.write(frame(2, b"second"));

        let mut second_request = writer.next().await.unwrap();
        assert_eq!(second_request.sequence(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_request.materialize_wire_bytes().unwrap(), b"second");
        second_request.complete(SinkWriteOutcome::Written);

        let mut deferred_request = writer.next().await.unwrap();
        assert_eq!(deferred_request.sequence(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            deferred_request.materialize_wire_bytes().unwrap(),
            b"sampled-at-write"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            deferred_request.materialize_wire_bytes().unwrap(),
            b"sampled-at-write"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        deferred_request.complete(SinkWriteOutcome::Written);

        assert_eq!(second.await, SinkWriteOutcome::Written);
        assert_eq!(deferred.await, SinkWriteOutcome::Written);
    }

    #[tokio::test]
    async fn deferred_materialization_failure_can_fail_the_writer_closed() {
        let (sink, mut writer) = started_writer(2).await;
        let failed = sink.enqueue_deferred(2, || Err("clock unavailable".to_owned()));
        let pending = sink.write(frame(3, b"must-not-overtake"));

        let mut request = writer.next().await.unwrap();
        let error = request.materialize_wire_bytes().unwrap_err();
        request.complete(definitely_not_written(&error));
        writer.fail_closed(&error);

        assert!(sink.is_closed());
        assert_eq!(
            failed.await,
            SinkWriteOutcome::DefinitelyNotWritten {
                error: "clock unavailable".to_owned(),
            }
        );
        assert!(matches!(
            pending.await,
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }
}
