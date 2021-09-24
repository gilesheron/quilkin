/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod config;
mod endpoint_chooser;
mod metrics;

use std::sync::Arc;

use crate::filters::{metadata::{CACHED_ENDPOINT, NEW_ENDPOINT}, prelude::*, DynFilterFactory};

use slog::{error, o, Logger};
use metrics::Metrics;

use config::ProtoConfig;
use endpoint_chooser::EndpointChooser;
use crate::endpoint::RetainedItems;

use std::net::SocketAddr;

pub use config::{Config, Policy};

pub const NAME: &str = "quilkin.extensions.filters.load_balancer.v1alpha1.LoadBalancer";

/// Returns a factory for creating load balancing filters.
pub fn factory(base: &Logger) -> DynFilterFactory {
    Box::from(LoadBalancerFilterFactory::new(base))
}

/// Balances packets over the upstream endpoints.
struct LoadBalancer {
    endpoint_chooser: Box<dyn EndpointChooser>,
    log: Logger,
    metrics: Metrics,
}

impl Filter for LoadBalancer {
    fn read(&self, mut ctx: ReadContext) -> Option<ReadResponse> {
        let endpoint: SocketAddr;

        match ctx.metadata.get(&Arc::new(CACHED_ENDPOINT.to_string())) {
            Some(value) => match value.downcast_ref::<SocketAddr>() {
                Some(ep_ref) => {
                    println!("found endpoint {:?} in cache", *ep_ref);
                    endpoint = *ep_ref;
                },
                None => {
                    error!(
                        self.log,
                        "Packets are being dropped as cached endpint has invalid type: expected SocketAddr";
                        "count" => self.metrics.packets_dropped_invalid_cached_endpoint.get()
                    );
                    self.metrics.packets_dropped_invalid_cached_endpoint.inc();
                    return None;
                }
            }
            None => {
                endpoint = self.endpoint_chooser
                                .choose_endpoints(&mut ctx.endpoints, ctx.from);
                println!("chosen endpoint {:?}", endpoint);
                ctx.metadata.insert(Arc::new(NEW_ENDPOINT.to_string()), Box::new(endpoint));
            }
        }

        match ctx.endpoints.retain(| ep | ep.address == endpoint) {
            RetainedItems::Some(count) => {
                println!("{} endpoints retained", count);
            },
            RetainedItems::All => {
                println!("All endpoints retained");
            },
            RetainedItems::None => {
                println!("No endpoints retained");
            },
        }

        Some(ctx.into())
    }
}

impl LoadBalancer {
    fn new(endpoint_chooser: Box<dyn EndpointChooser>, base: &Logger, metrics: Metrics) -> Self {
        Self {
            endpoint_chooser,
            log: base.new(o!("source" => "extensions::TokenRouter")),
            metrics,
        }
    }
}

/// Factory for the LoadBalancer filter
struct LoadBalancerFilterFactory {
    log: Logger,
}

impl LoadBalancerFilterFactory {
    pub fn new(base: &Logger) -> Self {
        LoadBalancerFilterFactory { log: base.clone() }
    }
}

impl FilterFactory for LoadBalancerFilterFactory {
    fn name(&self) -> &'static str {
        NAME
    }

    fn create_filter(&self, args: CreateFilterArgs) -> Result<Box<dyn Filter>, Error> {
        let config: Config = self
            .require_config(args.config)?
            .deserialize::<Config, ProtoConfig>(self.name())?;

        Ok(Box::new(LoadBalancer::new (
            config.policy.as_endpoint_chooser(),
            &self.log,
            Metrics::new(&args.metrics_registry)?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use crate::{
        endpoint::{Endpoint, Endpoints},
        filters::{
            load_balancer::LoadBalancerFilterFactory, CreateFilterArgs, Filter, FilterFactory,
            ReadContext,
        },
    };
    use prometheus::Registry;

    fn create_filter(config: &str) -> Box<dyn Filter> {
        let factory = LoadBalancerFilterFactory;
        factory
            .create_filter(CreateFilterArgs::fixed(
                Registry::default(),
                Some(&serde_yaml::from_str(config).unwrap()),
            ))
            .unwrap()
    }

    fn get_response_addresses(
        filter: &dyn Filter,
        input_addresses: &[SocketAddr],
        source: SocketAddr,
    ) -> Vec<SocketAddr> {
        filter
            .read(ReadContext::new(
                Endpoints::new(
                    input_addresses
                        .iter()
                        .map(|addr| Endpoint::new(*addr))
                        .collect(),
                )
                .unwrap()
                .into(),
                source,
                vec![],
            ))
            .unwrap()
            .endpoints
            .iter()
            .map(|ep| ep.address)
            .collect::<Vec<_>>()
    }

    #[test]
    fn round_robin_load_balancer_policy() {
        let addresses = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.2:8080".parse().unwrap(),
            "127.0.0.3:8080".parse().unwrap(),
        ];

        let yaml = "
policy: ROUND_ROBIN
";
        let filter = create_filter(yaml);

        // Check that we repeat the same addresses in sequence forever.
        let expected_sequence = addresses.iter().map(|addr| vec![*addr]).collect::<Vec<_>>();

        for _ in 0..10 {
            assert_eq!(
                expected_sequence,
                (0..addresses.len())
                    .map(|_| get_response_addresses(
                        filter.as_ref(),
                        &addresses,
                        "127.0.0.1:8080".parse().unwrap()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn random_load_balancer_policy() {
        let addresses = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.2:8080".parse().unwrap(),
            "127.0.0.3:8080".parse().unwrap(),
        ];

        let yaml = "
policy: RANDOM
";
        let filter = create_filter(yaml);

        // Run a few selection rounds through the addresses.
        let mut result_sequences = vec![];
        for _ in 0..10 {
            let sequence = (0..addresses.len())
                .map(|_| {
                    get_response_addresses(
                        filter.as_ref(),
                        &addresses,
                        "127.0.0.1:8080".parse().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            result_sequences.push(sequence);
        }

        // Check that every address was chosen at least once.
        assert_eq!(
            addresses.into_iter().collect::<HashSet<_>>(),
            result_sequences
                .clone()
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>(),
        );

        // Check that there is at least one different sequence of addresses.
        assert!(
            &result_sequences[1..]
                .iter()
                .any(|seq| seq != &result_sequences[0]),
            "the same sequence of addresses were chosen for random load balancer"
        );
    }

    #[test]
    fn hash_load_balancer_policy() {
        let addresses = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.2:8080".parse().unwrap(),
            "127.0.0.3:8080".parse().unwrap(),
        ];
        let source_ips = vec!["127.1.1.1", "127.2.2.2", "127.3.3.3"];
        let source_ports = vec!["11111", "22222", "33333", "44444", "55555"];

        let yaml = "
policy: HASH
";
        let filter = create_filter(yaml);

        // Run a few selection rounds through the addresses.
        let mut result_sequences = vec![];
        for _ in 0..10 {
            let sequence = (0..addresses.len())
                .map(|_| {
                    get_response_addresses(
                        filter.as_ref(),
                        &addresses,
                        "127.0.0.1:8080".parse().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            result_sequences.push(sequence);
        }

        // Verify that all packets went the same way
        assert_eq!(
            1,
            result_sequences
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>()
                .len(),
        );

        // Run a few selection rounds through the address
        // this time vary the port for a single IP
        let mut result_sequences = vec![];
        for port in &source_ports {
            let sequence = (0..addresses.len())
                .map(|_| {
                    get_response_addresses(
                        filter.as_ref(),
                        &addresses,
                        format!("127.0.0.1:{}", port).parse().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            result_sequences.push(sequence);
        }

        // Verify that more than 1 path was picked
        assert_ne!(
            1,
            result_sequences
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>()
                .len(),
        );

        // Run a few selection rounds through the addresses
        // This time vary the source IP and port
        let mut result_sequences = vec![];
        for ip in source_ips {
            for port in &source_ports {
                let sequence = (0..addresses.len())
                    .map(|_| {
                        get_response_addresses(
                            filter.as_ref(),
                            &addresses,
                            format!("{}:{}", ip, port).parse().unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                result_sequences.push(sequence);
            }
        }

        // Check that every address was chosen at least once.
        assert_eq!(
            addresses.into_iter().collect::<HashSet<_>>(),
            result_sequences
                .clone()
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>(),
        );

        // Check that there is at least one different sequence of addresses.
        assert!(
            &result_sequences[1..]
                .iter()
                .any(|seq| seq != &result_sequences[0]),
            "the same sequence of addresses were chosen for hash load balancer"
        );

        //
    }
}
