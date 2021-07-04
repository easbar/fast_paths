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

use std::collections::BinaryHeap;

use crate::constants::Weight;
use crate::constants::INVALID_EDGE;
use crate::constants::INVALID_NODE;
use crate::constants::WEIGHT_MAX;
use crate::constants::{EdgeId, NodeId};
use crate::fast_graph::FastGraph;
use crate::heap_item::HeapItem;
use crate::shortest_path::ShortestPath;
use crate::valid_flags::ValidFlags;

pub struct PathCalculator {
    num_nodes: usize,
    data_fwd: Vec<Data>,
    data_bwd: Vec<Data>,
    valid_flags_fwd: ValidFlags,
    valid_flags_bwd: ValidFlags,
    heap_fwd: BinaryHeap<HeapItem>,
    heap_bwd: BinaryHeap<HeapItem>,
}

impl PathCalculator {
    pub fn new(num_nodes: usize) -> Self {
        PathCalculator {
            num_nodes,
            data_fwd: (0..num_nodes).map(|_i| Data::new()).collect(),
            data_bwd: (0..num_nodes).map(|_i| Data::new()).collect(),
            valid_flags_fwd: ValidFlags::new(num_nodes),
            valid_flags_bwd: ValidFlags::new(num_nodes),
            heap_fwd: BinaryHeap::new(),
            heap_bwd: BinaryHeap::new(),
        }
    }

    pub fn calc_path(
        &mut self,
        graph: &FastGraph,
        start: NodeId,
        end: NodeId,
    ) -> Option<ShortestPath> {
        self.calc_path_multiple_endpoints(graph, vec![(start, 0)], end)
    }

    pub fn calc_path_multiple_endpoints(
        &mut self,
        graph: &FastGraph,
        starts: Vec<(NodeId, Weight)>,
        end: NodeId,
    ) -> Option<ShortestPath> {
        assert_eq!(
            graph.get_num_nodes(),
            self.num_nodes,
            "given graph has invalid node count"
        );
        for (id, _) in &starts {
            assert!(*id < self.num_nodes, "invalid start node");
        }
        assert!(end < self.num_nodes, "invalid end node");
        self.heap_fwd.clear();
        self.heap_bwd.clear();
        self.valid_flags_fwd.invalidate_all();
        self.valid_flags_bwd.invalidate_all();

        let mut best_weight = WEIGHT_MAX;
        let mut meeting_node = INVALID_NODE;

        starts
            .iter()
            .filter(|(id, weight)| *id == end && *weight < WEIGHT_MAX)
            .min_by_key(|(_, weight)| weight)
            .map(|(_, weight)| {
                best_weight = *weight;
                meeting_node = end;
            });

        for (id, weight) in starts {
            if weight < WEIGHT_MAX {
                self.update_node_fwd(id, weight, INVALID_NODE, INVALID_EDGE);
                self.heap_fwd.push(HeapItem::new(weight, id));
            }
        }
        self.update_node_bwd(end, 0, INVALID_NODE, INVALID_EDGE);
        self.heap_bwd.push(HeapItem::new(0, end));

        loop {
            if self.heap_fwd.is_empty() && self.heap_bwd.is_empty() {
                break;
            }
            loop {
                if self.heap_fwd.is_empty() {
                    break;
                }
                let curr = self.heap_fwd.pop().unwrap();
                if self.is_settled_fwd(curr.node_id) {
                    continue;
                }
                if curr.weight > best_weight {
                    break;
                }
                // stall on demand optimization
                if self.is_stallable_fwd(graph, curr) {
                    continue;
                }
                let begin = graph.begin_out_edges(curr.node_id);
                let end = graph.end_out_edges(curr.node_id);
                for edge_id in begin..end {
                    let adj = graph.edges_fwd[edge_id].adj_node;
                    let edge_weight = graph.edges_fwd[edge_id].weight;
                    let weight = curr.weight + edge_weight;
                    if weight < self.get_weight_fwd(adj) {
                        self.update_node_fwd(adj, weight, curr.node_id, edge_id);
                        self.heap_fwd.push(HeapItem::new(weight, adj));
                    }
                }
                self.data_fwd[curr.node_id].settled = true;
                if self.valid_flags_bwd.is_valid(curr.node_id)
                    && curr.weight + self.get_weight_bwd(curr.node_id) < best_weight
                {
                    best_weight = curr.weight + self.get_weight_bwd(curr.node_id);
                    meeting_node = curr.node_id;
                }
                break;
            }

            loop {
                if self.heap_bwd.is_empty() {
                    break;
                }
                let curr = self.heap_bwd.pop().unwrap();
                if self.is_settled_bwd(curr.node_id) {
                    continue;
                }
                if curr.weight > best_weight {
                    break;
                }
                // stall on demand optimization
                if self.is_stallable_bwd(graph, curr) {
                    continue;
                }
                let begin = graph.begin_in_edges(curr.node_id);
                let end = graph.end_in_edges(curr.node_id);
                for edge_id in begin..end {
                    let adj = graph.edges_bwd[edge_id].adj_node;
                    let edge_weight = graph.edges_bwd[edge_id].weight;
                    let weight = curr.weight + edge_weight;
                    if weight < self.get_weight_bwd(adj) {
                        self.update_node_bwd(adj, weight, curr.node_id, edge_id);
                        self.heap_bwd.push(HeapItem::new(weight, adj));
                    }
                }
                self.data_bwd[curr.node_id].settled = true;
                if self.valid_flags_fwd.is_valid(curr.node_id)
                    && curr.weight + self.get_weight_fwd(curr.node_id) < best_weight
                {
                    best_weight = curr.weight + self.get_weight_fwd(curr.node_id);
                    meeting_node = curr.node_id;
                }
                break;
            }
        }

        if meeting_node == INVALID_NODE {
            return None;
        } else {
            assert!(best_weight < WEIGHT_MAX);
            let node_ids = self.extract_nodes(graph, end, meeting_node);
            let chosen_start = node_ids[0];
            return Some(ShortestPath::new(chosen_start, end, best_weight, node_ids));
        }
    }

    fn is_stallable_fwd(&self, graph: &FastGraph, curr: HeapItem) -> bool {
        let begin = graph.begin_in_edges(curr.node_id);
        let end = graph.end_in_edges(curr.node_id);
        for edge_id in begin..end {
            let adj = graph.edges_bwd[edge_id].adj_node;
            let adj_weight = self.get_weight_fwd(adj);
            if adj_weight == WEIGHT_MAX {
                continue;
            }
            let edge_weight = graph.edges_bwd[edge_id].weight;
            if adj_weight + edge_weight < curr.weight {
                return true;
            }
        }
        return false;
    }

    fn is_stallable_bwd(&self, graph: &FastGraph, curr: HeapItem) -> bool {
        let begin = graph.begin_out_edges(curr.node_id);
        let end = graph.end_out_edges(curr.node_id);
        for edge_id in begin..end {
            let adj = graph.edges_fwd[edge_id].adj_node;
            let adj_weight = self.get_weight_bwd(adj);
            if adj_weight == WEIGHT_MAX {
                continue;
            }
            let edge_weight = graph.edges_fwd[edge_id].weight;
            if adj_weight + edge_weight < curr.weight {
                return true;
            }
        }
        return false;
    }

    fn extract_nodes(&self, graph: &FastGraph, end: NodeId, meeting_node: NodeId) -> Vec<NodeId> {
        assert_ne!(meeting_node, INVALID_NODE);
        assert!(self.valid_flags_fwd.is_valid(meeting_node));
        assert!(self.valid_flags_bwd.is_valid(meeting_node));
        let mut result = Vec::new();
        let mut node = meeting_node;
        while self.data_fwd[node].inc_edge != INVALID_EDGE {
            PathCalculator::unpack_fwd(graph, &mut result, self.data_fwd[node].inc_edge, true);
            node = self.data_fwd[node].parent;
        }
        result.reverse();
        node = meeting_node;
        while self.data_bwd[node].inc_edge != INVALID_EDGE {
            PathCalculator::unpack_bwd(graph, &mut result, self.data_bwd[node].inc_edge, false);
            node = self.data_bwd[node].parent;
        }
        result.push(end);
        result
    }

    fn unpack_fwd(graph: &FastGraph, nodes: &mut Vec<NodeId>, edge_id: EdgeId, reverse: bool) {
        if !graph.edges_fwd[edge_id].is_shortcut() {
            nodes.push(graph.edges_fwd[edge_id].base_node);
            return;
        }
        if reverse {
            PathCalculator::unpack_fwd(
                graph,
                nodes,
                graph.edges_fwd[edge_id].replaced_out_edge,
                reverse,
            );
            PathCalculator::unpack_bwd(
                graph,
                nodes,
                graph.edges_fwd[edge_id].replaced_in_edge,
                reverse,
            );
        } else {
            PathCalculator::unpack_bwd(
                graph,
                nodes,
                graph.edges_fwd[edge_id].replaced_in_edge,
                reverse,
            );
            PathCalculator::unpack_fwd(
                graph,
                nodes,
                graph.edges_fwd[edge_id].replaced_out_edge,
                reverse,
            );
        }
    }

    fn unpack_bwd(graph: &FastGraph, nodes: &mut Vec<NodeId>, edge_id: EdgeId, reverse: bool) {
        if !graph.edges_bwd[edge_id].is_shortcut() {
            nodes.push(graph.edges_bwd[edge_id].adj_node);
            return;
        }
        if reverse {
            PathCalculator::unpack_fwd(
                graph,
                nodes,
                graph.edges_bwd[edge_id].replaced_out_edge,
                reverse,
            );
            PathCalculator::unpack_bwd(
                graph,
                nodes,
                graph.edges_bwd[edge_id].replaced_in_edge,
                reverse,
            );
        } else {
            PathCalculator::unpack_bwd(
                graph,
                nodes,
                graph.edges_bwd[edge_id].replaced_in_edge,
                reverse,
            );
            PathCalculator::unpack_fwd(
                graph,
                nodes,
                graph.edges_bwd[edge_id].replaced_out_edge,
                reverse,
            );
        }
    }

    fn update_node_fwd(&mut self, node: NodeId, weight: Weight, parent: NodeId, inc_edge: EdgeId) {
        self.valid_flags_fwd.set_valid(node);
        self.data_fwd[node].settled = false;
        self.data_fwd[node].weight = weight;
        self.data_fwd[node].parent = parent;
        self.data_fwd[node].inc_edge = inc_edge;
    }

    fn update_node_bwd(&mut self, node: NodeId, weight: Weight, parent: NodeId, inc_edge: EdgeId) {
        self.valid_flags_bwd.set_valid(node);
        self.data_bwd[node].settled = false;
        self.data_bwd[node].weight = weight;
        self.data_bwd[node].parent = parent;
        self.data_bwd[node].inc_edge = inc_edge;
    }

    fn is_settled_fwd(&self, node: NodeId) -> bool {
        self.valid_flags_fwd.is_valid(node) && self.data_fwd[node].settled
    }

    fn is_settled_bwd(&self, node: NodeId) -> bool {
        self.valid_flags_bwd.is_valid(node) && self.data_bwd[node].settled
    }

    fn get_weight_fwd(&self, node: NodeId) -> Weight {
        if self.valid_flags_fwd.is_valid(node) {
            self.data_fwd[node].weight
        } else {
            WEIGHT_MAX
        }
    }

    fn get_weight_bwd(&self, node: NodeId) -> Weight {
        if self.valid_flags_bwd.is_valid(node) {
            self.data_bwd[node].weight
        } else {
            WEIGHT_MAX
        }
    }
}

struct Data {
    settled: bool,
    weight: Weight,
    parent: NodeId,
    inc_edge: usize,
}

impl Data {
    fn new() -> Self {
        Data {
            settled: false,
            weight: WEIGHT_MAX,
            parent: INVALID_NODE,
            inc_edge: INVALID_EDGE,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fast_graph::FastGraphEdge;

    use super::*;

    #[test]
    fn unpack_fwd_single() {
        // 0 -> 1
        let mut g = FastGraph::new(2);
        g.edges_fwd
            .push(FastGraphEdge::new(0, 1, 3, INVALID_EDGE, INVALID_EDGE));
        let mut nodes = vec![];
        PathCalculator::unpack_fwd(&g, &mut nodes, 0, false);
        assert_eq!(nodes, vec![0]);
    }

    #[test]
    fn unpack_fwd_simple() {
        // 0 -> 1 -> 2
        let mut g = FastGraph::new(3);
        g.edges_fwd
            .push(FastGraphEdge::new(0, 1, 2, INVALID_EDGE, INVALID_EDGE));
        g.edges_fwd.push(FastGraphEdge::new(0, 2, 5, 0, 0));
        g.edges_bwd
            .push(FastGraphEdge::new(2, 1, 3, INVALID_EDGE, INVALID_EDGE));
        g.first_edge_ids_fwd = vec![0, 2, 0, 0];
        let mut nodes = vec![];
        PathCalculator::unpack_fwd(&g, &mut nodes, 1, false);
        assert_eq!(nodes, vec![1, 0]);
    }
}
