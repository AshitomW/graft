//! Exponentially weighted moving average filter for smoothing metrics.

/// Smoothing filter computing exponential moving averages.
#[derive(Debug, Clone, Copy)]
pub struct EwmaFilter {
    alpha: f64,
    current_value: Option<f64>,
}

impl EwmaFilter {
    /// Creates an EWMA filter with the specified smoothing coefficient.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.01, 1.0),
            current_value: None,
        }
    }

    /// Feeds a new numeric sample into the filter.
    pub fn update(&mut self, sample: f64) -> f64 {
        let new_val = match self.current_value {
            Some(prev) => self.alpha * sample + (1.0 - self.alpha) * prev,
            None => sample,
        };
        self.current_value = Some(new_val);
        new_val
    }

    /// Returns the current smoothed value, if any samples have been recorded.
    pub fn value(&self) -> Option<f64> {
        self.current_value
    }

    /// Resets the filter to uninitialized state.
    pub fn reset(&mut self) {
        self.current_value = None;
    }
}
