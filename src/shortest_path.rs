/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::constants::NodeId;
use crate::constants::Weight;
use crate::constants::WEIGHT_MAX;
use crate::constants::WEIGHT_ZERO;

#[derive(Debug)]
pub struct ShortestPath {
    source: NodeId,
    target: NodeId,
    weight: Weight,
    nodes: Vec<NodeId>,
}

impl PartialEq for ShortestPath {
    fn eq(&self, other: &ShortestPath) -> bool {
        self.source == other.source && self.target == other.target && self.weight == other.weight
        // do not insist on equal nodes arrays, because there can be unambiguous shortest paths
    }
}

impl ShortestPath {
    pub fn new(source: NodeId, target: NodeId, weight: Weight, nodes: Vec<NodeId>) -> Self {
        ShortestPath {
            source,
            target,
            weight,
            nodes,
        }
    }

    pub fn singular(node: NodeId) -> Self {
        ShortestPath {
            source: node,
            target: node,
            weight: WEIGHT_ZERO,
            nodes: vec![node],
        }
    }

    pub fn none(source: NodeId, target: NodeId) -> Self {
        ShortestPath {
            source,
            target,
            weight: WEIGHT_MAX,
            nodes: vec![],
        }
    }

    pub fn get_source(&self) -> NodeId {
        self.source
    }

    pub fn get_target(&self) -> NodeId {
        self.target
    }

    pub fn get_weight(&self) -> Weight {
        self.weight
    }

    pub fn get_nodes(&self) -> &Vec<NodeId> {
        &self.nodes
    }

    pub fn is_found(&self) -> bool {
        self.weight != WEIGHT_MAX
    }
}
