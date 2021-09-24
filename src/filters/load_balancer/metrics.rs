/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{IntCounterVec, Registry, Result as MetricsResult};

use crate::metrics::{filter_opts, CollectorExt};

/// Register and manage metrics for this filter
pub(super) struct Metrics {
    pub(super) packets_dropped_invalid_cached_endpoint: GenericCounter<AtomicU64>,
}

impl Metrics {
    pub(super) fn new(registry: &Registry) -> MetricsResult<Self> {
        let label_names = vec!["reason"];
        let metric = IntCounterVec::new(
            filter_opts(
                "packets_dropped",
                "LoadBalancer",
                "Total number of packets dropped. labels: reason.",
            ),
            &label_names,
        )?
        .register(registry)?;

        Ok(Metrics {
            packets_dropped_invalid_cached_endpoint: metric
                .get_metric_with_label_values(vec!["InvalidCachedEndpoint"].as_slice())?,
        })
    }
}
