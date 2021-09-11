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
use crate::constants::{NodeId, INVALID_NODE, WEIGHT_MAX, WEIGHT_ZERO};
use crate::heap_item::HeapItem;
use crate::preparation_graph::PreparationGraph;
use crate::valid_flags::ValidFlags;

pub struct WitnessSearch {
    num_nodes: usize,
    data: Vec<Data>,
    valid_flags: ValidFlags,
    heap: BinaryHeap<HeapItem>,
    start_node: NodeId,
    avoid_node: NodeId,
}

impl WitnessSearch {
    pub fn new(num_nodes: usize) -> Self {
        let heap = BinaryHeap::new();
        WitnessSearch {
            num_nodes,
            data: (0..num_nodes).map(|_i| Data::new()).collect(),
            valid_flags: ValidFlags::new(num_nodes),
            heap,
            start_node: INVALID_NODE,
            avoid_node: INVALID_NODE,
        }
    }

    /// Initializes the witness search for a given start and avoid node. Calling this method
    /// resets/clears previously calculated data.
    pub fn init(&mut self, start: NodeId, avoid_node: NodeId) {
        assert!(start != INVALID_NODE, "the start node must be valid");
        assert!(
            start != avoid_node,
            "path calculation must not start with avoided node"
        );
        self.start_node = start;
        self.avoid_node = avoid_node;

        self.heap.clear();
        self.valid_flags.invalidate_all();
        self.update_node(start, 0);
        self.heap.push(HeapItem::new(0, start));
    }

    /// Returns an upper bound for the shortest path weight between the start node and a given target
    /// node.
    /// Calling this method runs Dijkstra's algorithm for the given start_node. The avoid_node will
    /// never be visited. There are multiple criteria that make the search stop:
    ///   1) the target is settled. the returned weight will be the actual shortest path weight.
    ///   2) the next node to be settled exceeds the given weight_limit. the returned weight will
    ///      be the best known upper bound for the real shortest path weight at this point. it will
    ///      always be larger than weight_limit in this case.
    ///   3) the tentative weight of the target is found to be equal or smaller than weight_limit.
    ///      this way the search can be stopped without finding the actual shortest path as soon as
    ///      any path with weight <= weight_limit has been found.
    /// The shortest path tree established during the search will be re-used until the init
    /// function is called again.
    pub fn find_max_weight(
        &mut self,
        graph: &PreparationGraph,
        target: NodeId,
        weight_limit: Weight,
    ) -> Weight {
        assert_eq!(
            graph.get_num_nodes(),
            self.num_nodes,
            "given graph has invalid node count"
        );
        assert!(
            self.start_node != INVALID_NODE,
            "the start node must be valid, call init() before find_max_weight()"
        );
        assert!(
            self.start_node != self.avoid_node && target != self.avoid_node,
            "path calculation must not start or end with avoided node"
        );
        if target == self.start_node {
            return WEIGHT_ZERO;
        }
        if self.valid_flags.is_valid(target)
            && (self.data[target].settled || self.data[target].weight <= weight_limit)
        {
            return self.data[target].weight;
        }
        while !self.heap.is_empty() {
            let curr = *self.heap.peek().unwrap();
            if curr.weight > weight_limit {
                break;
            }
            self.heap.pop();
            if self.is_settled(curr.node_id) {
                // todo: since we are not using a special decrease key operation yet we need to
                // filter out duplicate heap items here
                continue;
            }
            let mut found_target = false;
            for i in 0..graph.out_edges[curr.node_id].len() {
                let adj = graph.out_edges[curr.node_id][i].adj_node;
                if adj == self.avoid_node {
                    continue;
                }
                let edge_weight = graph.out_edges[curr.node_id][i].weight;
                let weight = curr.weight + edge_weight;
                if weight < self.get_current_weight(adj) {
                    self.update_node(adj, weight);
                    self.heap.push(HeapItem::new(weight, adj));
                    if adj == target && weight <= weight_limit {
                        found_target = true;
                    }
                }
            }
            self.data[curr.node_id].settled = true;
            if found_target || curr.node_id == target {
                break;
            }
        }
        self.get_current_weight(target)
    }

    fn update_node(&mut self, node: NodeId, weight: Weight) {
        self.valid_flags.set_valid(node);
        self.data[node].settled = false;
        self.data[node].weight = weight;
    }

    fn is_settled(&self, node: NodeId) -> bool {
        self.valid_flags.is_valid(node) && self.data[node].settled
    }

    fn get_current_weight(&self, node: NodeId) -> Weight {
        if self.valid_flags.is_valid(node) {
            self.data[node].weight
        } else {
            WEIGHT_MAX
        }
    }
}

struct Data {
    settled: bool,
    weight: Weight,
}

impl Data {
    fn new() -> Self {
        // todo: initializing with these values is not strictly necessary
        Data {
            settled: false,
            weight: WEIGHT_MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avoid_node() {
        // 0 -> 1 -> 2
        // |         |
        // 3 -> 4 -> 5
        let mut g = PreparationGraph::new(6);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(0, 3, 10);
        g.add_edge(3, 4, 1);
        g.add_edge(4, 5, 1);
        g.add_edge(5, 2, 1);
        let mut ws = WitnessSearch::new(g.get_num_nodes());
        ws.init(0, INVALID_NODE);
        assert_eq!(2, ws.find_max_weight(&g, 2, 2));
        assert_eq!(2, ws.find_max_weight(&g, 2, 2));
        ws.init(0, 1);
        assert_eq!(13, ws.find_max_weight(&g, 2, 13));
    }

    #[test]
    fn limit_weight() {
        // 0 -> 1 -> 2 -> 3 -> 4
        let mut g = PreparationGraph::new(5);
        for i in 0..4 {
            g.add_edge(i, i + 1, 1);
        }
        let mut ws = WitnessSearch::new(g.get_num_nodes());
        ws.init(0, INVALID_NODE);
        assert_eq!(3, ws.find_max_weight(&g, 3, 3));
        // reset and reduce weight limit to 2. node 3 will not be settled, but we still get 3 as
        // upper bound for the weight of node 3
        ws.init(0, INVALID_NODE);
        assert_eq!(3, ws.find_max_weight(&g, 3, 2));
        // .. but not for node 4
        assert_eq!(WEIGHT_MAX, ws.find_max_weight(&g, 4, 2));
        // if the weight has already be calculated no further search is required
        assert_eq!(2, ws.find_max_weight(&g, 2, 2));
        assert_eq!(2, ws.find_max_weight(&g, 2, 2));
        assert_eq!(2, ws.find_max_weight(&g, 2, 2));
        // ... even when the weight limit is smaller than previously
        assert_eq!(2, ws.find_max_weight(&g, 2, 1));
        // we can extend the current search space
        assert_eq!(4, ws.find_max_weight(&g, 4, 3));
        assert_eq!(4, ws.find_max_weight(&g, 4, 4));
        assert_eq!(4, ws.find_max_weight(&g, 4, 5));
    }

    #[test]
    fn stop_early() {
        // 0 -> 1 -> 2 -> 3
        //  \----------->/
        let mut g = PreparationGraph::new(4);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(2, 3, 1);
        g.add_edge(0, 3, 4);
        let mut ws = WitnessSearch::new(g.get_num_nodes());
        ws.init(0, INVALID_NODE);
        // the shortest path weight is 3, but since we set the limit to 10 the alternative path
        // 0->3 with weight 4 that we find earlier is 'good enough' and find_max_weight returns
        assert_eq!(4, ws.find_max_weight(&g, 3, 10));
        // calling the same again still does not trigger an expansion of the search tree
        assert_eq!(4, ws.find_max_weight(&g, 3, 10));
        // this is still true when we reduce the weight limit to the weight of the sub-optimal path
        assert_eq!(4, ws.find_max_weight(&g, 3, 4));
        assert_eq!(4, ws.find_max_weight(&g, 3, 4));
        // when we further reduce the weight limit the search needs to be more accurate
        assert_eq!(3, ws.find_max_weight(&g, 3, 3));
        // ... and repeating the search yields the same result
        assert_eq!(3, ws.find_max_weight(&g, 3, 3));
    }
}
