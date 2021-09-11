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

use serde::Deserialize;
use serde::Serialize;

use crate::constants::Weight;
use crate::constants::{EdgeId, NodeId, INVALID_EDGE};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FastGraph {
    num_nodes: usize,
    pub(crate) ranks: Vec<usize>,
    pub(crate) edges_fwd: Vec<FastGraphEdge>,
    pub(crate) first_edge_ids_fwd: Vec<EdgeId>,

    pub(crate) edges_bwd: Vec<FastGraphEdge>,
    pub(crate) first_edge_ids_bwd: Vec<EdgeId>,
}

impl FastGraph {
    pub fn new(num_nodes: usize) -> Self {
        FastGraph {
            ranks: vec![0; num_nodes],
            num_nodes,
            edges_fwd: vec![],
            first_edge_ids_fwd: vec![0; num_nodes + 1],
            edges_bwd: vec![],
            first_edge_ids_bwd: vec![0; num_nodes + 1],
        }
    }

    pub fn get_node_ordering(&self) -> Vec<NodeId> {
        let mut ordering = vec![0; self.ranks.len()];
        for i in 0..self.ranks.len() {
            ordering[self.ranks[i]] = i;
        }
        ordering
    }

    pub fn get_num_nodes(&self) -> usize {
        self.num_nodes
    }

    pub fn get_num_out_edges(&self) -> usize {
        self.edges_fwd.len()
    }

    pub fn get_num_in_edges(&self) -> usize {
        self.edges_bwd.len()
    }

    pub fn begin_in_edges(&self, node: NodeId) -> usize {
        self.first_edge_ids_bwd[self.ranks[node]]
    }

    pub fn end_in_edges(&self, node: NodeId) -> usize {
        self.first_edge_ids_bwd[self.ranks[node] + 1]
    }

    pub fn begin_out_edges(&self, node: NodeId) -> usize {
        self.first_edge_ids_fwd[self.ranks[node]]
    }

    pub fn end_out_edges(&self, node: NodeId) -> usize {
        self.first_edge_ids_fwd[self.ranks[node] + 1]
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct FastGraphEdge {
    // todo: the base_node is 'redundant' for the routing query so to say, but makes the implementation easier for now
    // and can still be removed at a later time, we definitely need this information on original
    // edges for shortcut unpacking. a possible hack is storing it in the (for non-shortcuts)
    // unused replaced_in/out_edge field.
    pub base_node: NodeId,
    pub adj_node: NodeId,
    pub weight: Weight,
    pub replaced_in_edge: EdgeId,
    pub replaced_out_edge: EdgeId,
}

impl FastGraphEdge {
    pub fn new(
        base_node: NodeId,
        adj_node: NodeId,
        weight: Weight,
        replaced_edge1: EdgeId,
        replaced_edge2: EdgeId,
    ) -> Self {
        FastGraphEdge {
            base_node,
            adj_node,
            weight,
            replaced_in_edge: replaced_edge1,
            replaced_out_edge: replaced_edge2,
        }
    }

    pub fn is_shortcut(&self) -> bool {
        assert!(
            (self.replaced_in_edge == INVALID_EDGE && self.replaced_out_edge == INVALID_EDGE)
                || (self.replaced_in_edge != INVALID_EDGE
                    && self.replaced_out_edge != INVALID_EDGE)
        );
        self.replaced_in_edge != INVALID_EDGE
    }
}
