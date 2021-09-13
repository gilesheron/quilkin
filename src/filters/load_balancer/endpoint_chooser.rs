/*
 * Copyright 2021 Google LLC
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

use std::sync::atomic::{AtomicUsize, Ordering};

use rand::{thread_rng, Rng};

crate::include_proto!("endpoint");
use self::endpoint::get_endpoint_client::GetEndpointClient;
use self::endpoint::EndpointRequest;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use std::sync::mpsc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::endpoint::UpstreamEndpoints;

/// EndpointChooser chooses from a set of endpoints that a proxy is connected to.
pub trait EndpointChooser: Send + Sync {
    /// choose_endpoints asks for the next endpoint(s) to use.
    fn choose_endpoints(&self, endpoints: &mut UpstreamEndpoints, from: SocketAddr) -> SocketAddr;
}

/// RoundRobinEndpointChooser chooses endpoints in round-robin order.
pub struct RoundRobinEndpointChooser {
    next_endpoint: AtomicUsize,
}

impl RoundRobinEndpointChooser {
    pub fn new() -> Self {
        RoundRobinEndpointChooser {
            next_endpoint: AtomicUsize::new(0),
        }
    }
}

impl EndpointChooser for RoundRobinEndpointChooser {
    fn choose_endpoints(&self, endpoints: &mut UpstreamEndpoints, _from: SocketAddr) -> SocketAddr {
        let count = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        // Note: Unwrap is safe here because the index is guaranteed to be in range.
        let num_endpoints = endpoints.size();
        endpoints.get(count % num_endpoints)
            .expect("BUG: unwrap should have been safe because index into endpoints list should be in range")
    }
}

/// RandomEndpointChooser chooses endpoints in random order.
pub struct RandomEndpointChooser;

impl EndpointChooser for RandomEndpointChooser {
    fn choose_endpoints(&self, endpoints: &mut UpstreamEndpoints, _from: SocketAddr) -> SocketAddr {
        // Note: Unwrap is safe here because the index is guaranteed to be in range.
        let idx = (&mut thread_rng()).gen_range(0..endpoints.size());
        endpoints.get(idx)
            .expect("BUG: unwrap should have been safe because index into endpoints list should be in range")
    }
}

/// HashEndpointChooser chooses endpoints based on a hash of source IP and port.
pub struct HashEndpointChooser;

impl EndpointChooser for HashEndpointChooser {
    fn choose_endpoints(&self, endpoints: &mut UpstreamEndpoints, from: SocketAddr) -> SocketAddr {
        let num_endpoints = endpoints.size();
        let mut hasher = DefaultHasher::new();
        from.hash(&mut hasher);
        endpoints.get(hasher.finish() as usize % num_endpoints)
            .expect("BUG: unwrap should have been safe because index into endpoints list should be in range")
    }
}

/// ControlPlaneEndpointChooser chooses endpoints from a Control Plane API Call
pub struct ApiEndpointChooser {
    txmt: Mutex<mpsc::Sender<(SocketAddr, oneshot::Sender<SocketAddr>)>>,
}

impl ApiEndpointChooser {
    pub fn new() -> Self {
        println!("new API chooser");
        let (tx, rx) = mpsc::channel::<(SocketAddr, oneshot::Sender<SocketAddr>)>();

        // Spawn API client thread
        std::thread::spawn( move || {
            ApiEndpointChooser::run(rx);
        });
        ApiEndpointChooser { txmt: Mutex::new(tx.clone()) }
    }

    fn run(rx: mpsc::Receiver<(SocketAddr, oneshot::Sender<SocketAddr>)>) {
        println!("run");

        //  Create and enter Tokio runtime for API client thread
        let rt = tokio::runtime::Runtime::new().unwrap();
        let eg = rt.enter();

        // API thread runs async (as uses tonic etc.) and never terminates
        let fut = async {
            match GetEndpointClient::connect("http://127.0.0.1:50051").await {
                Ok(mut client) => {
                    println!("connected to API");
                    loop {
                        let _result = match rx.recv() {
                            Ok((message, channel)) => {
                                println!("got message");

                                let request = tonic::Request::new(
                                    EndpointRequest {
                                        req: message.to_string(),
                                    }
                                );

                                match client.send(request).await {
                                    Ok(response) => {
                                        println!("RESPONSE={:?}", response);

                                        let msg = response.into_inner().res;
                                        let socket_addr: SocketAddr = msg.parse().unwrap();

                                        match channel.send(socket_addr) {
                                            Ok(()) => {
                                                println!("Response sent ok");
                                            },
                                            Err(e) => {
                                                println!("Oneshot error {:?}", e);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        println!("Send Error {:?}", e);
                                    },
                                }
                            },
                            Err(e) => {
                                println!("No message received - error {:}", e);
                            }
                        };
                    }
                },
                Err(e) => {
                    println!("Connect Error {:?}", e);
                },
            };
        };

        futures::executor::block_on(fut);

        // Compiler needs this but thread will never terminate....
        drop(eg);
    }

    async fn msg(sender: mpsc::Sender<(SocketAddr, oneshot::Sender<SocketAddr>)>, from: SocketAddr) -> SocketAddr {
        let (shoot_out, shoot_in) = oneshot::channel::<SocketAddr>();
        let message = (from, shoot_out);

        // Send message to API thread
        match sender.send(message) {
            Ok(()) => {
                println!("sent ok")
            },
            Err(e) => {
                println!("send error {:?}", e)
            },
        }

        // Get oneshot response
        match shoot_in.await {
            Ok(response) => {
                println!("Received ok {}", response.to_string());
                return response
            },
            Err(e) => {
                println!("Receive Error {:?}", e);
                return "0.0.0.0:0".parse().unwrap()
            },
        }
    }
}

impl EndpointChooser for ApiEndpointChooser {
    fn choose_endpoints(&self, _endpoints: &mut UpstreamEndpoints, from: SocketAddr) -> SocketAddr {
        println!("ApiEndpointChooser");

        // Need to add caching here

        let sender = self.txmt.lock().unwrap().clone();
        let source = from.clone();

        // Make async call from sync fn (we know we're actually running under Tokio)
        let handle = tokio::runtime::Handle::current();
        let _eg = handle.enter();
        return futures::executor::block_on(ApiEndpointChooser::msg(sender, source))
    }
}
