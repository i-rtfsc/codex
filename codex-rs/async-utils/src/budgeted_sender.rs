use async_channel::Receiver;
use async_channel::SendError;
use async_channel::Sender;
use async_channel::TrySendError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use tokio_util::sync::CancellationToken;

type Weight<T> = fn(&T) -> Result<usize, &'static str>;

struct Usage {
    count: usize,
    bytes: usize,
    failure: Option<&'static str>,
}

struct Budget<T> {
    capacity: usize,
    max_bytes: usize,
    weight: Weight<T>,
    usage: Mutex<Usage>,
    failed: CancellationToken,
}

/// Optional fail-closed admission for channels whose producers cannot await
/// consumer capacity without blocking their own control or shutdown path.
pub struct BudgetedSender<T> {
    sender: Sender<T>,
    budget: Option<Arc<Budget<T>>>,
}

impl<T> Clone for BudgetedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            budget: self.budget.clone(),
        }
    }
}

impl<T> From<Sender<T>> for BudgetedSender<T> {
    fn from(sender: Sender<T>) -> Self {
        Self {
            sender,
            budget: None,
        }
    }
}

impl<T> BudgetedSender<T> {
    /// `weight` must be pure and deterministic for each immutable queued value.
    /// A budget violation closes admission and wakes `failed`, never waits for capacity.
    pub fn bounded(capacity: usize, max_bytes: usize, weight: Weight<T>) -> (Self, Receiver<T>) {
        assert!(capacity > 0 && max_bytes > 0);
        let (sender, receiver) = async_channel::bounded(capacity);
        (
            Self {
                sender,
                budget: Some(Arc::new(Budget {
                    capacity,
                    max_bytes,
                    weight,
                    usage: Mutex::new(Usage {
                        count: 0,
                        bytes: 0,
                        failure: None,
                    }),
                    failed: CancellationToken::new(),
                })),
            },
            receiver,
        )
    }

    pub fn is_budgeted(&self) -> bool {
        self.budget.is_some()
    }

    pub fn failure(&self) -> Option<&'static str> {
        self.budget
            .as_ref()
            .and_then(|budget| self.usage(budget).failure)
    }

    pub async fn failed(&self) {
        match &self.budget {
            Some(budget) => budget.failed.cancelled().await,
            None => std::future::pending().await,
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let Some(budget) = &self.budget else {
            return self.sender.send(value).await;
        };
        let weight = (budget.weight)(&value);
        let mut usage = self.usage(budget);
        if usage.failure.is_some() || self.sender.is_closed() {
            return Err(SendError(value));
        }
        let bytes = match weight {
            Ok(bytes) => bytes,
            Err(reason) => {
                self.fail(budget, &mut usage, reason);
                return Err(SendError(value));
            }
        };
        let failure = if usage.count >= budget.capacity {
            Some("event queue count budget exceeded")
        } else if bytes
            .checked_add(usage.bytes)
            .is_none_or(|total| total > budget.max_bytes)
        {
            Some("event queue byte budget exceeded")
        } else {
            None
        };
        if let Some(reason) = failure {
            self.fail(budget, &mut usage, reason);
            return Err(SendError(value));
        }
        match self.sender.try_send(value) {
            Ok(()) => {
                usage.count += 1;
                usage.bytes += bytes;
                Ok(())
            }
            Err(TrySendError::Full(value)) => {
                self.fail(
                    budget,
                    &mut usage,
                    "event queue admission accounting failed",
                );
                Err(SendError(value))
            }
            Err(TrySendError::Closed(value)) => Err(SendError(value)),
        }
    }

    fn fail(&self, budget: &Budget<T>, usage: &mut Usage, reason: &'static str) {
        usage.failure.get_or_insert(reason);
        self.sender.close();
        budget.failed.cancel();
    }

    fn usage<'a>(&self, budget: &'a Budget<T>) -> MutexGuard<'a, Usage> {
        match budget.usage.lock() {
            Ok(usage) => usage,
            Err(error) => {
                let mut usage = error.into_inner();
                self.fail(budget, &mut usage, "event queue budget owner poisoned");
                usage
            }
        }
    }

    /// The sole consumer releases admission only after removing the same
    /// immutable value from the channel. `weight` must be deterministic.
    pub fn received(&self, value: &T) {
        let Some(budget) = &self.budget else { return };
        let weight = (budget.weight)(value);
        let mut usage = self.usage(budget);
        match weight {
            Ok(bytes) => match (usage.count.checked_sub(1), usage.bytes.checked_sub(bytes)) {
                (Some(count), Some(bytes)) => {
                    usage.count = count;
                    usage.bytes = bytes;
                }
                _ => self.fail(
                    budget,
                    &mut usage,
                    "event queue admission accounting failed",
                ),
            },
            Err(reason) => self.fail(budget, &mut usage, reason),
        }
    }
}

#[cfg(test)]
#[path = "budgeted_sender_tests.rs"]
mod tests;
