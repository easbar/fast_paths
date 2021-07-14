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
use crate::shortest_path::ShortestPath;
use crate::valid_flags::ValidFlags;

pub struct Dijkstra {
    num_nodes: usize,
    data: Vec<Data>,
    valid_flags: ValidFlags,
    heap: BinaryHeap<HeapItem>,
    avoid_node: NodeId,
    max_weight: Weight,
    max_nodes: usize,
    start_node: NodeId,
}

impl Dijkstra {
    pub fn new(num_nodes: usize) -> Self {
        let heap = BinaryHeap::new();
        Dijkstra {
            num_nodes,
            data: (0..num_nodes).map(|_i| Data::new()).collect(),
            valid_flags: ValidFlags::new(num_nodes),
            heap,
            avoid_node: INVALID_NODE,
            max_weight: WEIGHT_MAX,
            max_nodes: usize::MAX,
            start_node: INVALID_NODE,
        }
    }

    pub fn avoid_node(&mut self, node: NodeId) {
        self.avoid_node = node;
        self.start_node = INVALID_NODE;
    }

    pub fn set_max_weight(&mut self, weight: Weight) {
        self.max_weight = weight;
    }

    pub fn set_max_nodes(&mut self, nodes: usize) {
        self.max_nodes = nodes;
    }

    #[allow(dead_code)]
    pub fn calc_path(
        &mut self,
        graph: &PreparationGraph,
        start: NodeId,
        end: NodeId,
    ) -> Option<ShortestPath> {
        self.do_calc_path(graph, start, end);
        self.build_path(start, end)
    }

    pub fn calc_weight(
        &mut self,
        graph: &PreparationGraph,
        start: NodeId,
        end: NodeId,
    ) -> Option<Weight> {
        self.do_calc_path(graph, start, end);
        if start == end {
            return Some(WEIGHT_ZERO);
        }
        let weight = self.get_final_weight(end);
        if weight == WEIGHT_MAX {
            None
        } else {
            Some(weight)
        }
    }

    fn do_calc_path(&mut self, graph: &PreparationGraph, start: NodeId, end: NodeId) {
        assert_eq!(
            graph.get_num_nodes(),
            self.num_nodes,
            "given graph has invalid node count"
        );
        assert!(
            start != self.avoid_node && end != self.avoid_node,
            "path calculation must not start or end with avoided node"
        );
        if start == end {
            return;
        }
        if start != self.start_node {
            self.heap.clear();
            self.valid_flags.invalidate_all();
            self.update_node(start, 0, INVALID_NODE);
            self.heap.push(HeapItem::new(0, start));
        }
        if self.is_settled(end) {
            return;
        }
        self.start_node = start;

        let mut popped = 0;
        while !self.heap.is_empty() {
            let curr = self.heap.pop().unwrap();
            popped += 1;
            if popped > self.max_nodes {
                break;
            }
            if self.is_settled(curr.node_id) {
                // todo: since we are not using a special decrease key operation yet we need to
                // filter out duplicate heap items here
                continue;
            }
            for i in 0..graph.out_edges[curr.node_id].len() {
                let adj = graph.out_edges[curr.node_id][i].adj_node;
                let edge_weight = graph.out_edges[curr.node_id][i].weight;
                if adj == self.avoid_node {
                    continue;
                }
                let weight = curr.weight + edge_weight;
                if weight < self.get_weight(adj) {
                    self.update_node(adj, weight, curr.node_id);
                    self.heap.push(HeapItem::new(weight, adj));
                }
            }
            self.data[curr.node_id].settled = true;
            if curr.node_id == end {
                break;
            }
            if curr.weight >= self.max_weight {
                break;
            }
        }
    }

    fn build_path(&mut self, start: NodeId, end: NodeId) -> Option<ShortestPath> {
        if start == end {
            return Some(ShortestPath::singular(start));
        }
        let weight = self.get_final_weight(end);
        if weight == WEIGHT_MAX {
            return None;
        }
        let mut path = Vec::new();
        let mut node = end;
        while self.data[node].parent != INVALID_NODE {
            path.push(node);
            node = self.data[node].parent;
        }
        path.push(start);
        path = path.iter().rev().cloned().collect();
        Some(ShortestPath::new(start, end, weight, path))
    }

    fn get_final_weight(&mut self, end: usize) -> usize {
        if !self.valid_flags.is_valid(end) ||
            // if max_weight is exceeded we might have found some path to the end node, but since
            // it is not necessarily the shortest we return WEIGHT_MAX (not found) also in this case
            self.data[end].weight > self.max_weight
        {
            WEIGHT_MAX
        } else {
            self.data[end].weight
        }
    }

    fn update_node(&mut self, node: NodeId, weight: Weight, parent: NodeId) {
        self.valid_flags.set_valid(node);
        self.data[node].settled = false;
        self.data[node].weight = weight;
        self.data[node].parent = parent;
    }

    fn is_settled(&self, node: NodeId) -> bool {
        self.valid_flags.is_valid(node) && self.data[node].settled
    }

    fn get_weight(&self, node: NodeId) -> Weight {
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
    parent: NodeId,
}

impl Data {
    fn new() -> Self {
        // todo: initializing with these values is not strictly necessary
        Data {
            settled: false,
            weight: WEIGHT_MAX,
            parent: INVALID_NODE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_path() {
        //      7 -> 8 -> 9
        //      |         |
        // 0 -> 5 -> 6 -  |
        // |         |  \ |
        // 1 -> 2 -> 3 -> 4
        let mut g = PreparationGraph::new(10);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(2, 3, 1);
        g.add_edge(3, 4, 20);
        g.add_edge(0, 5, 5);
        g.add_edge(5, 6, 1);
        g.add_edge(6, 4, 20);
        g.add_edge(6, 3, 20);
        g.add_edge(5, 7, 5);
        g.add_edge(7, 8, 1);
        g.add_edge(8, 9, 1);
        g.add_edge(9, 4, 1);
        let mut d = Dijkstra::new(g.get_num_nodes());
        assert_no_path(&mut d, &g, 4, 0);
        assert_path(&mut d, &g, 4, 4, 0, vec![4]);
        assert_path(&mut d, &g, 6, 3, 20, vec![6, 3]);
        assert_path(&mut d, &g, 1, 4, 22, vec![1, 2, 3, 4]);
        assert_path(&mut d, &g, 0, 4, 13, vec![0, 5, 7, 8, 9, 4]);
    }

    #[test]
    fn go_around() {
        // 0 -> 1
        // |    |
        // 2 -> 3
        let mut g = PreparationGraph::new(4);
        g.add_edge(0, 1, 10);
        g.add_edge(0, 2, 1);
        g.add_edge(2, 3, 1);
        g.add_edge(3, 1, 1);
        let mut d = Dijkstra::new(g.get_num_nodes());
        assert_path(&mut d, &g, 0, 1, 3, vec![0, 2, 3, 1]);
    }

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
        let mut d = Dijkstra::new(g.get_num_nodes());
        assert_path(&mut d, &g, 0, 2, 2, vec![0, 1, 2]);
        assert_path(&mut d, &g, 0, 2, 2, vec![0, 1, 2]);
        d.avoid_node(1);
        assert_path(&mut d, &g, 0, 2, 13, vec![0, 3, 4, 5, 2]);
    }

    #[test]
    fn limit_weight() {
        // 0 -> 1 -> 2 -> 3 -> 4
        let mut g = PreparationGraph::new(5);
        for i in 0..4 {
            g.add_edge(i, i + 1, 1);
        }
        let mut d = Dijkstra::new(g.get_num_nodes());
        assert_path(&mut d, &g, 0, 4, 4, vec![0, 1, 2, 3, 4]);
        d.set_max_weight(2);
        assert_no_path(&mut d, &g, 0, 4);
        assert_no_path(&mut d, &g, 0, 3);
        assert_path(&mut d, &g, 0, 2, 2, vec![0, 1, 2]);
        d.set_max_weight(3);
        assert_path(&mut d, &g, 0, 3, 3, vec![0, 1, 2, 3]);
    }

    #[test]
    fn run_multiple() {
        // 0 -> 1 -> 2
        //       \
        //         3 -> 4
        //        / \
        //   7 <-6   |-> 5
        //            \
        //             8 -> 9 -> 10
        let mut g = PreparationGraph::new(11);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(1, 3, 1);
        g.add_edge(3, 4, 1);
        g.add_edge(3, 6, 1);
        g.add_edge(6, 7, 1);
        g.add_edge(3, 5, 1);
        g.add_edge(3, 8, 1);
        g.add_edge(8, 9, 1);
        g.add_edge(9, 10, 1);
        let mut d = Dijkstra::new(g.get_num_nodes());
        // make sure that if we use the same source node multiple times the shortest path tree
        // is re-used correctly     ,
        assert_path(&mut d, &g, 0, 1, 1, vec![0, 1]);
        assert_path(&mut d, &g, 0, 2, 2, vec![0, 1, 2]);
        assert_path(&mut d, &g, 0, 4, 3, vec![0, 1, 3, 4]);
        assert_path(&mut d, &g, 0, 3, 2, vec![0, 1, 3]);
        assert_path(&mut d, &g, 0, 7, 4, vec![0, 1, 3, 6, 7]);
        assert_path(&mut d, &g, 0, 5, 3, vec![0, 1, 3, 5]);
        assert_path(&mut d, &g, 0, 10, 5, vec![0, 1, 3, 8, 9, 10]);

        // if we use another source node everything is reset correctly
        assert_path(&mut d, &g, 3, 10, 3, vec![3, 8, 9, 10]);
    }

    fn assert_no_path(
        dijkstra: &mut Dijkstra,
        graph: &PreparationGraph,
        source: NodeId,
        target: NodeId,
    ) {
        assert_eq!(dijkstra.calc_path(&graph, source, target), None);
        assert_eq!(dijkstra.calc_weight(&graph, source, target), None);
    }

    fn assert_path(
        dijkstra: &mut Dijkstra,
        graph: &PreparationGraph,
        source: NodeId,
        target: NodeId,
        weight: Weight,
        nodes: Vec<NodeId>,
    ) {
        let dijkstra_path = dijkstra.calc_path(&graph, source, target);
        assert_eq!(
            dijkstra_path,
            Some(ShortestPath::new(source, target, weight, nodes.clone()))
        );
        // ShortestPath PartialEq does not consider nodes!
        assert_eq!(nodes, dijkstra_path.unwrap().get_nodes().clone());
        assert_eq!(dijkstra.calc_weight(&graph, source, target), Some(weight));
    }
}
