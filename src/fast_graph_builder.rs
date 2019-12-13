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

use std::cmp::max;
use std::collections::BTreeSet;

use priority_queue::PriorityQueue;

use crate::constants::Weight;
use crate::constants::{EdgeId, NodeId, INVALID_EDGE, INVALID_NODE};
use crate::fast_graph::FastGraphEdge;

use super::dijkstra::Dijkstra;
use super::fast_graph::FastGraph;
use super::input_graph::InputGraph;
use super::preparation_graph::PreparationGraph;
use crate::node_contractor;

pub struct FastGraphBuilder {
    fast_graph: FastGraph,
    num_nodes: usize,
    center_nodes_fwd: Vec<NodeId>,
    center_nodes_bwd: Vec<NodeId>,
}

impl FastGraphBuilder {
    fn new(input_graph: &InputGraph) -> Self {
        FastGraphBuilder {
            fast_graph: FastGraph::new(input_graph.get_num_nodes()),
            num_nodes: input_graph.get_num_nodes(),
            center_nodes_fwd: vec![],
            center_nodes_bwd: vec![],
        }
    }

    pub fn build(input_graph: &InputGraph) -> FastGraph {
        FastGraphBuilder::build_with_params(input_graph, &Params::default())
    }

    pub fn build_with_params(input_graph: &InputGraph, params: &Params) -> FastGraph {
        let mut builder = FastGraphBuilder::new(input_graph);
        builder.run_contraction(input_graph, params);
        builder.fast_graph
    }

    pub fn build_with_order(
        input_graph: &InputGraph,
        order: &Vec<NodeId>,
    ) -> Result<FastGraph, String> {
        if input_graph.get_num_nodes() != order.len() {
            return Err(String::from(
                "The given order must have as many nodes as the input graph",
            ));
        }
        let mut builder = FastGraphBuilder::new(input_graph);
        builder.run_contraction_with_order(input_graph, order);
        Ok(builder.fast_graph)
    }

    fn run_contraction(&mut self, input_graph: &InputGraph, params: &Params) {
        let mut preparation_graph = PreparationGraph::from_input_graph(input_graph);
        let mut dijkstra = Dijkstra::new(self.num_nodes);
        let mut levels = vec![0; self.num_nodes];
        let mut queue = PriorityQueue::new();
        for node in 0..self.num_nodes {
            let priority = -node_contractor::calc_relevance(
                &mut preparation_graph,
                params,
                &mut dijkstra,
                node,
                0,
            );
            queue.push(node, priority as Weight);
        }
        let mut rank = 0;
        while !queue.is_empty() {
            let node = queue.pop().unwrap().0;
            let mut neighbors = BTreeSet::new();
            for out_edge in &preparation_graph.out_edges[node] {
                neighbors.insert(out_edge.adj_node);
                self.fast_graph.edges_fwd.push(FastGraphEdge::new(
                    node,
                    out_edge.adj_node,
                    out_edge.weight,
                    INVALID_EDGE,
                    INVALID_EDGE,
                ));
                self.center_nodes_fwd.push(out_edge.center_node);
            }
            self.fast_graph.first_edge_ids_fwd[rank + 1] = self.fast_graph.get_num_out_edges();

            for in_edge in &preparation_graph.in_edges[node] {
                neighbors.insert(in_edge.adj_node);
                self.fast_graph.edges_bwd.push(FastGraphEdge::new(
                    node,
                    in_edge.adj_node,
                    in_edge.weight,
                    INVALID_EDGE,
                    INVALID_EDGE,
                ));
                self.center_nodes_bwd.push(in_edge.center_node)
            }
            self.fast_graph.first_edge_ids_bwd[rank + 1] = self.fast_graph.get_num_in_edges();

            self.fast_graph.ranks[rank] = node;
            node_contractor::contract_node(&mut preparation_graph, &mut dijkstra, node);
            for neighbor in neighbors {
                levels[neighbor] = max(levels[neighbor], levels[node] + 1);
                let priority = -node_contractor::calc_relevance(
                    &mut preparation_graph,
                    params,
                    &mut dijkstra,
                    neighbor,
                    levels[neighbor],
                ) as Weight;
                queue.change_priority(&neighbor, priority);
            }
            //            println!("contracted node {} / {}, num edges fwd: {}, num edges bwd: {}", rank+1, self.num_nodes, self.fast_graph.get_num_out_edges(), self.fast_graph.get_num_in_edges());
            rank += 1;
        }
        self.finish_contraction();
    }

    fn run_contraction_with_order(&mut self, input_graph: &InputGraph, order: &Vec<NodeId>) {
        let mut preparation_graph = PreparationGraph::from_input_graph(input_graph);
        let mut dijkstra = Dijkstra::new(self.num_nodes);
        for rank in 0..order.len() {
            let node = order[rank];
            if node >= self.num_nodes {
                panic!("Order contains invalid node id: {}", node);
            }
            for out_edge in &preparation_graph.out_edges[node] {
                self.fast_graph.edges_fwd.push(FastGraphEdge::new(
                    node,
                    out_edge.adj_node,
                    out_edge.weight,
                    INVALID_EDGE,
                    INVALID_EDGE,
                ));
                self.center_nodes_fwd.push(out_edge.center_node);
            }
            self.fast_graph.first_edge_ids_fwd[rank + 1] = self.fast_graph.get_num_out_edges();

            for in_edge in &preparation_graph.in_edges[node] {
                self.fast_graph.edges_bwd.push(FastGraphEdge::new(
                    node,
                    in_edge.adj_node,
                    in_edge.weight,
                    INVALID_EDGE,
                    INVALID_EDGE,
                ));
                self.center_nodes_bwd.push(in_edge.center_node)
            }
            self.fast_graph.first_edge_ids_bwd[rank + 1] = self.fast_graph.get_num_in_edges();

            self.fast_graph.ranks[rank] = node;
            node_contractor::contract_node(&mut preparation_graph, &mut dijkstra, node);
            //            println!("contracted node {} / {}, num edges fwd: {}, num edges bwd: {}", rank+1, self.num_nodes, self.fast_graph.get_num_out_edges(), self.fast_graph.get_num_in_edges());
        }
        self.finish_contraction();
    }

    fn finish_contraction(&mut self) {
        let ranks_copy = self.fast_graph.ranks.clone();
        for i in 0..ranks_copy.len() {
            self.fast_graph.ranks[ranks_copy[i]] = i;
        }

        for i in 0..self.num_nodes {
            for edge_id in self.fast_graph.begin_out_edges(i)..self.fast_graph.end_out_edges(i) {
                let c = self.center_nodes_fwd[edge_id];
                if c == INVALID_NODE {
                    self.fast_graph.edges_fwd[edge_id].replaced_in_edge = INVALID_EDGE;
                    self.fast_graph.edges_fwd[edge_id].replaced_out_edge = INVALID_EDGE;
                } else {
                    self.fast_graph.edges_fwd[edge_id].replaced_in_edge = self.get_in_edge_id(c, i);
                    self.fast_graph.edges_fwd[edge_id].replaced_out_edge =
                        self.get_out_edge_id(c, self.fast_graph.edges_fwd[edge_id].adj_node);
                }
            }
        }

        for i in 0..self.num_nodes {
            for edge_id in self.fast_graph.begin_in_edges(i)..self.fast_graph.end_in_edges(i) {
                let c = self.center_nodes_bwd[edge_id];
                if c == INVALID_NODE {
                    self.fast_graph.edges_bwd[edge_id].replaced_in_edge = INVALID_EDGE;
                    self.fast_graph.edges_bwd[edge_id].replaced_out_edge = INVALID_EDGE;
                } else {
                    self.fast_graph.edges_bwd[edge_id].replaced_in_edge =
                        self.get_in_edge_id(c, self.fast_graph.edges_bwd[edge_id].adj_node);
                    self.fast_graph.edges_bwd[edge_id].replaced_out_edge =
                        self.get_out_edge_id(c, i);
                }
            }
        }
    }

    fn get_out_edge_id(&self, node: NodeId, adj_node: NodeId) -> EdgeId {
        for edge_id in self.fast_graph.begin_out_edges(node)..self.fast_graph.end_out_edges(node) {
            if self.fast_graph.edges_fwd[edge_id].adj_node == adj_node {
                return edge_id;
            }
        }
        panic!["could not find out-edge id"]
    }

    fn get_in_edge_id(&self, node: NodeId, adj_node: NodeId) -> EdgeId {
        for edge_id in self.fast_graph.begin_in_edges(node)..self.fast_graph.end_in_edges(node) {
            if self.fast_graph.edges_bwd[edge_id].adj_node == adj_node {
                return edge_id;
            }
        }
        panic!["could not find in-edge id"]
    }
}

pub struct Params {
    pub hierarchy_depth_factor: f32,
    pub edge_quotient_factor: f32,
}

impl Params {
    pub fn new(ratio: f32) -> Self {
        Params {
            hierarchy_depth_factor: ratio,
            edge_quotient_factor: 1.0,
        }
    }

    pub fn default() -> Self {
        Params::new(0.1)
    }
}

#[cfg(test)]
mod tests {
    use crate::shortest_path::ShortestPath;

    use super::*;
    use crate::{calc_path, prepare_with_order};

    #[test]
    fn calc_path_linear_bwd_only() {
        // 2->0->1
        let mut g = InputGraph::new();
        g.add_edge(2, 0, 9);
        g.add_edge(0, 1, 49);
        g.freeze();
        let fast_graph = prepare_with_order(&g, &vec![0, 1, 2]).unwrap();
        assert_path(&fast_graph, 2, 1, 58, vec![2, 0, 1]);
    }

    #[test]
    fn calc_path_linear_fwd_only() {
        // 1->0->2
        let mut g = InputGraph::new();
        g.add_edge(1, 0, 9);
        g.add_edge(0, 2, 49);
        g.freeze();
        let fast_graph = prepare_with_order(&g, &vec![0, 1, 2]).unwrap();
        assert_path(&fast_graph, 1, 2, 58, vec![1, 0, 2]);
    }

    #[test]
    fn calc_path_simple() {
        //   --->------4
        //  /          |
        // 0 - 1 - 2 - 3
        let mut g = InputGraph::new();
        g.add_edge_bidir(0, 1, 5);
        g.add_edge_bidir(1, 2, 3);
        g.add_edge_bidir(2, 3, 2);
        g.add_edge_bidir(3, 4, 6);
        g.add_edge(0, 4, 2);
        g.freeze();

        let fast_graph = prepare_with_order(&g, &vec![0, 1, 2, 3, 4]).unwrap();
        assert_path(&fast_graph, 0, 4, 2, vec![0, 4]);
        assert_path(&fast_graph, 4, 0, 16, vec![4, 3, 2, 1, 0]);
        assert_path(&fast_graph, 1, 4, 7, vec![1, 0, 4]);
        assert_path(&fast_graph, 2, 4, 8, vec![2, 3, 4]);
    }

    #[test]
    fn calc_path_another() {
        // 4
        // |  \
        // 0 -> 2
        // |    |
        // 3  - 1
        let mut g = InputGraph::new();
        g.add_edge(0, 2, 1);
        g.add_edge(0, 4, 9);
        g.add_edge(1, 3, 3);
        g.add_edge(2, 1, 8);
        g.add_edge(3, 0, 4);
        g.add_edge(3, 1, 8);
        g.add_edge(4, 2, 4);
        g.freeze();

        let fast_graph = prepare_with_order(&g, &vec![0, 1, 2, 3, 4]).unwrap();
        assert_path(&fast_graph, 4, 3, 15, vec![4, 2, 1, 3]);
    }

    fn assert_path(
        fast_graph: &FastGraph,
        source: NodeId,
        target: NodeId,
        weight: Weight,
        nodes: Vec<NodeId>,
    ) {
        assert_eq!(
            calc_path(fast_graph, source, target),
            Some(ShortestPath::new(source, target, weight, nodes))
        );
    }
}
