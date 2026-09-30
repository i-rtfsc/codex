use async_channel::Receiver;
use codex_async_utils::budgeted_sender::BudgetedSender;
use codex_protocol::protocol::Event;
use std::io::Write;

pub(crate) const EVENT_QUEUE_CAPACITY: usize = 512;
pub(crate) const EVENT_QUEUE_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const EVENT_BYTES: usize = 128 * 1024;

pub(crate) fn channel() -> (BudgetedSender<Event>, Receiver<Event>) {
    if cfg!(feature = "native-bounded-events") {
        BudgetedSender::bounded(EVENT_QUEUE_CAPACITY, EVENT_QUEUE_BYTES, event_weight)
    } else {
        let (sender, receiver) = async_channel::unbounded();
        (sender.into(), receiver)
    }
}

struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > EVENT_BYTES - self.0 {
            return Err(std::io::Error::other("native event size budget exceeded"));
        }
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn event_weight(event: &Event) -> Result<usize, &'static str> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, event)
        .map_err(|_| "native event serialization or size budget exceeded")?;
    Ok(counter.0)
}

#[cfg(test)]
#[path = "event_queue_tests.rs"]
mod tests;
