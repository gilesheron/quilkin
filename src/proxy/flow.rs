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

use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Clone, Default)]
pub struct FlowCache {
    flow_cache: HashMap<SocketAddr, SocketAddr>,
}

impl FlowCache {
    /// Creates a new flow cache
    /// 
    pub fn new() -> Self {
        Self {
            flow_cache: HashMap::new(),
        }
    }

    /// gets a flow cache entry for a key
    pub fn get(&mut self, key: &SocketAddr) -> Option<&SocketAddr> {
        return self.flow_cache.get(key)
    }

    /// adds a flow entry to the cache
    pub fn insert(&mut self, key: &SocketAddr, value: &SocketAddr) {
        match self.flow_cache.insert(*key, *value) {
            None => {},
            Some (_value) => {return},
        }
    }
}
