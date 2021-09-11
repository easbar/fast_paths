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

use std::cmp::{max, Reverse};
use std::collections::BTreeSet;

use priority_queue::PriorityQueue;

use crate::constants::Weight;
use crate::constants::{EdgeId, NodeId, INVALID_EDGE, INVALID_NODE};
use crate::fast_graph::FastGraphEdge;

use super::fast_graph::FastGraph;
use super::input_graph::InputGraph;
use super::preparation_graph::PreparationGraph;
use crate::node_contractor;
use crate::witness_search::WitnessSearch;

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
        order: &[NodeId],
    ) -> Result<FastGraph, String> {
        FastGraphBuilder::build_with_order_with_params(
            input_graph,
            order,
            &ParamsWithOrder::default(),
        )
    }

    pub fn build_with_order_with_params(
        input_graph: &InputGraph,
        order: &[NodeId],
        params: &ParamsWithOrder,
    ) -> Result<FastGraph, String> {
        if input_graph.get_num_nodes() != order.len() {
            return Err(String::from(
                "The given order must have as many nodes as the input graph",
            ));
        }
        let mut builder = FastGraphBuilder::new(input_graph);
        builder.run_contraction_with_order(input_graph, order, params);
        Ok(builder.fast_graph)
    }

    fn run_contraction(&mut self, input_graph: &InputGraph, params: &Params) {
        let mut preparation_graph = PreparationGraph::from_input_graph(input_graph);
        let mut witness_search = WitnessSearch::new(self.num_nodes);
        let mut levels = vec![0; self.num_nodes];
        let mut queue = PriorityQueue::new();
        for node in 0..self.num_nodes {
            let priority = node_contractor::calc_relevance(
                &mut preparation_graph,
                params,
                &mut witness_search,
                node,
                0,
                params.max_settled_nodes_initial_relevance,
            ) as Weight;
            queue.push(node, Reverse(priority));
        }
        let mut rank = 0;
        while !queue.is_empty() {
            // This normally yields the greatest priority, but since we use Reverse, it's the
            // least.
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

            self.fast_graph.ranks[node] = rank;
            node_contractor::contract_node(
                &mut preparation_graph,
                &mut witness_search,
                node,
                params.max_settled_nodes_contraction,
            );
            for neighbor in neighbors {
                levels[neighbor] = max(levels[neighbor], levels[node] + 1);
                let priority = node_contractor::calc_relevance(
                    &mut preparation_graph,
                    params,
                    &mut witness_search,
                    neighbor,
                    levels[neighbor],
                    params.max_settled_nodes_neighbor_relevance,
                ) as Weight;
                queue.change_priority(&neighbor, Reverse(priority));
            }
            debug!(
                "contracted node {} / {}, num edges fwd: {}, num edges bwd: {}",
                rank + 1,
                self.num_nodes,
                self.fast_graph.get_num_out_edges(),
                self.fast_graph.get_num_in_edges()
            );
            rank += 1;
        }
        self.finish_contraction();
    }

    fn run_contraction_with_order(
        &mut self,
        input_graph: &InputGraph,
        order: &[NodeId],
        params: &ParamsWithOrder,
    ) {
        let mut preparation_graph = PreparationGraph::from_input_graph(input_graph);
        let mut witness_search = WitnessSearch::new(self.num_nodes);
        for (rank, node) in order.iter().cloned().enumerate() {
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

            self.fast_graph.ranks[node] = rank;
            node_contractor::contract_node(
                &mut preparation_graph,
                &mut witness_search,
                node,
                params.max_settled_nodes_contraction_with_order,
            );
            debug!(
                "contracted node {} / {}, num edges fwd: {}, num edges bwd: {}",
                rank + 1,
                self.num_nodes,
                self.fast_graph.get_num_out_edges(),
                self.fast_graph.get_num_in_edges()
            );
        }
        self.finish_contraction();
    }

    fn finish_contraction(&mut self) {
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
        panic!("could not find out-edge id")
    }

    fn get_in_edge_id(&self, node: NodeId, adj_node: NodeId) -> EdgeId {
        for edge_id in self.fast_graph.begin_in_edges(node)..self.fast_graph.end_in_edges(node) {
            if self.fast_graph.edges_bwd[edge_id].adj_node == adj_node {
                return edge_id;
            }
        }
        panic!("could not find in-edge id")
    }
}

pub struct Params {
    pub hierarchy_depth_factor: f32,
    pub edge_quotient_factor: f32,
    /// The maximum number of settled nodes per witness search performed when priorities are
    /// calculated for all nodes initially. Since this does not take much time normally you should
    /// probably keep the default.
    pub max_settled_nodes_initial_relevance: usize,
    /// The maximum number of settled nodes per witness search performed when updating priorities
    /// of neighbor nodes after a node was contracted. The preparation time can strongly depend on
    /// this value and even setting it to 0 might be feasible. Higher values (like 500+) should
    /// yield less shortcuts and faster query times at the cost of a longer preparation time. Lower
    /// values (like 0-100) should yield faster preparation at the cost of slower query times and
    /// more shortcuts. To know for sure you should still make your own experiments for your
    /// specific graph.
    pub max_settled_nodes_neighbor_relevance: usize,
    /// The maximum number of settled nodes per witness search when contracting a node. Higher values
    /// like 500+ mean less shortcuts (fast graph edges), slower preparation and faster queries while
    /// lower values mean more shortcuts, slower queries and faster preparation.
    pub max_settled_nodes_contraction: usize,
}

impl Params {
    pub fn new(
        ratio: f32,
        max_settled_nodes_initial_relevance: usize,
        max_settled_nodes_neighbor_relevance: usize,
        max_settled_nodes_contraction: usize,
    ) -> Self {
        Params {
            hierarchy_depth_factor: ratio,
            edge_quotient_factor: 1.0,
            max_settled_nodes_initial_relevance,
            max_settled_nodes_neighbor_relevance,
            max_settled_nodes_contraction,
        }
    }

    pub fn default() -> Self {
        Params {
            hierarchy_depth_factor: 0.1,
            edge_quotient_factor: 1.0,
            max_settled_nodes_initial_relevance: 100,
            max_settled_nodes_neighbor_relevance: 3,
            max_settled_nodes_contraction: 100,
        }
    }
}

pub struct ParamsWithOrder {
    /// The maximum number of settled nodes per witness search when contracting a node. Smaller
    /// values mean slower queries, more shortcuts, but faster preparation time. Note that the
    /// performance also can strongly depend on the relation between this parameter and
    /// Params::max_settled_nodes_contraction that was used to build the FastGraph and obtain the
    /// node ordering initially. In most cases you should use the same value for these two parameters.
    pub max_settled_nodes_contraction_with_order: usize,
}

impl ParamsWithOrder {
    pub fn new(max_settled_nodes_contraction_with_order: usize) -> Self {
        ParamsWithOrder {
            max_settled_nodes_contraction_with_order,
        }
    }

    pub fn default() -> Self {
        ParamsWithOrder::new(100)
    }
}

#[cfg(test)]
mod tests {
    use crate::shortest_path::ShortestPath;

    use super::*;
    // todo: maybe move these tests and the ones in lib.rs into the 'tests' folder as integration tests
    //       see rust docs
    use crate::{
        calc_path, create_calculator, prepare, prepare_with_order, PathCalculator, WEIGHT_MAX,
    };

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
        let fast_path = calc_path(fast_graph, source, target);
        assert_eq!(
            fast_path,
            Some(ShortestPath::new(source, target, weight, nodes.clone()))
        );
        // ShortestPath PartialEq does not consider nodes!
        assert_eq!(nodes, fast_path.unwrap().get_nodes().clone(),);
    }

    #[test]
    fn multiple_sources() {
        // 0 -> 1 -> 2 <- 5
        // 3 -> 4 ->/
        let mut input_graph = InputGraph::new();
        input_graph.add_edge(0, 1, 3);
        input_graph.add_edge(1, 2, 4);
        input_graph.add_edge(3, 4, 2);
        input_graph.add_edge(4, 2, 3);
        input_graph.add_edge(5, 2, 2);
        input_graph.freeze();
        let fast_graph = prepare(&input_graph);
        let mut path_calculator = create_calculator(&fast_graph);
        // two different options for source, without initial weight
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(0, 0), (3, 0)],
            vec![(2, 0)],
            vec![3, 4, 2],
            5,
        );
        // two different options for source, with initial weights, 0->1->2's weight is higher,
        // but since the initial weight is smaller it is the shortest path
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(0, 1), (3, 4)],
            vec![(2, 0)],
            vec![0, 1, 2],
            8,
        );
        // one option appearing twice with different initial weights, the smaller one should be taken
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(0, 5), (0, 3)],
            vec![(2, 0)],
            vec![0, 1, 2],
            10,
        );
        // ... now put the smaller weight first
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(5, 10), (5, 20)],
            vec![(2, 0)],
            vec![5, 2],
            12,
        );
        // start options equal the target
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(1, 10), (1, 1)],
            vec![(1, 0)],
            vec![1],
            1,
        );
        // one of the start options equals the target, but still the shortest path is another one
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(2, 10), (0, 0)],
            vec![(2, 0)],
            vec![0, 1, 2],
            7,
        );
        // start options with max weight cannot yield a shortest path
        assert_path_multiple_sources_and_targets_not_found(
            &mut path_calculator,
            &fast_graph,
            vec![(1, WEIGHT_MAX)],
            vec![(1, 0)],
        );
        // .. or at least they are ignored in case there are other ones
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(1, WEIGHT_MAX), (0, 3)],
            vec![(1, 0)],
            vec![0, 1],
            6,
        );
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(1, WEIGHT_MAX), (3, 3)],
            vec![(2, 0)],
            vec![3, 4, 2],
            8,
        );
    }

    #[test]
    fn multiple_targets() {
        // 0 <- 1 <- 2
        // 3 <- 4 <-/
        let mut input_graph = InputGraph::new();
        input_graph.add_edge(1, 0, 3);
        input_graph.add_edge(2, 1, 4);
        input_graph.add_edge(4, 3, 2);
        input_graph.add_edge(2, 4, 3);
        input_graph.freeze();
        let fast_graph = prepare(&input_graph);
        let mut path_calculator = create_calculator(&fast_graph);
        // two different options for target, without initial weight
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(2, 0)],
            vec![(0, 0), (3, 0)],
            vec![2, 4, 3],
            5,
        );
        // two different options for target, with initial weight
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(2, 0)],
            vec![(0, 0), (3, 1)],
            vec![2, 4, 3],
            6,
        );
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(2, 0)],
            vec![(0, 0), (3, 3)],
            vec![2, 1, 0],
            7,
        );
        // start==end
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(4, 0)],
            vec![(4, 3), (4, 1)],
            vec![4],
            1,
        )
    }

    #[test]
    fn multiple_sources_and_targets() {
        // 0 -- 1 -- 2 -- 3 -- 4
        // 5 -- 6 --/ \-- 7 -- 8
        let mut input_graph = InputGraph::new();
        input_graph.add_edge_bidir(0, 1, 1);
        input_graph.add_edge_bidir(1, 2, 2);
        input_graph.add_edge_bidir(2, 3, 3);
        input_graph.add_edge_bidir(3, 4, 4);
        input_graph.add_edge_bidir(5, 6, 5);
        input_graph.add_edge_bidir(6, 2, 6);
        input_graph.add_edge_bidir(2, 7, 7);
        input_graph.add_edge_bidir(7, 8, 8);
        input_graph.freeze();
        let fast_graph = prepare(&input_graph);
        let mut path_calculator = create_calculator(&fast_graph);
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(1, 7), (6, 2), (5, 6)],
            vec![(3, 1), (4, 9), (5, 7)],
            vec![6, 2, 3],
            12,
        );
        assert_path_multiple_sources_and_targets(
            &mut path_calculator,
            &fast_graph,
            vec![(1, 7), (6, 2)],
            vec![(1, 9), (6, 3)],
            vec![6],
            5,
        );
    }

    fn assert_path_multiple_sources_and_targets(
        path_calculator: &mut PathCalculator,
        fast_graph: &FastGraph,
        sources: Vec<(NodeId, Weight)>,
        targets: Vec<(NodeId, Weight)>,
        expected_nodes: Vec<NodeId>,
        expected_weight: Weight,
    ) {
        let fast_path =
            path_calculator.calc_path_multiple_sources_and_targets(fast_graph, sources, targets);
        assert!(fast_path.is_some());
        let p = fast_path.unwrap();
        assert_eq!(expected_nodes, p.get_nodes().clone(), "unexpected nodes");
        assert_eq!(expected_weight, p.get_weight(), "unexpected weight");
    }

    fn assert_path_multiple_sources_and_targets_not_found(
        path_calculator: &mut PathCalculator,
        fast_graph: &FastGraph,
        sources: Vec<(NodeId, Weight)>,
        targets: Vec<(NodeId, Weight)>,
    ) {
        let fast_path =
            path_calculator.calc_path_multiple_sources_and_targets(&fast_graph, sources, targets);
        assert!(fast_path.is_none(), "there should be no path");
    }
}
