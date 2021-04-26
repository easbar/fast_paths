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

use crate::constants::Weight;
use crate::constants::{NodeId, INVALID_NODE};
use crate::input_graph::InputGraph;

pub struct PreparationGraph {
    pub out_edges: Vec<Vec<Arc>>,
    pub in_edges: Vec<Vec<Arc>>,
    num_nodes: usize,
}

impl PreparationGraph {
    pub fn new(num_nodes: usize) -> Self {
        let out_edges: Vec<Vec<Arc>> = (0..num_nodes).map(|_| Vec::with_capacity(3)).collect();
        let in_edges = out_edges.clone();
        PreparationGraph {
            out_edges,
            in_edges,
            num_nodes,
        }
    }

    pub fn from_input_graph(input_graph: &InputGraph) -> Self {
        let mut graph = PreparationGraph::new(input_graph.get_num_nodes());
        for e in input_graph.get_edges() {
            graph.add_edge(e.from, e.to, e.weight);
        }
        graph
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId, weight: Weight) {
        self.add_edge_or_shortcut(from, to, weight, INVALID_NODE);
    }

    pub fn add_edge_or_shortcut(
        &mut self,
        from: NodeId,
        to: NodeId,
        weight: Weight,
        center_node: NodeId,
    ) {
        self.assert_valid_node_id(to);
        self.out_edges[from].push(Arc::new(to, weight, center_node));
        self.in_edges[to].push(Arc::new(from, weight, center_node));
    }

    pub fn add_or_reduce_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        weight: Weight,
        center_node: NodeId,
    ) {
        if self.reduce_edge(from, to, weight, center_node) {
            return;
        }
        self.add_edge_or_shortcut(from, to, weight, center_node);
    }

    fn reduce_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        weight: Weight,
        center_node: NodeId,
    ) -> bool {
        for out_edge in &mut self.out_edges[from] {
            if out_edge.adj_node == to {
                if out_edge.weight <= weight {
                    return true;
                }
                for in_edge in &mut self.in_edges[to] {
                    if in_edge.adj_node == from {
                        out_edge.weight = weight;
                        in_edge.weight = weight;
                        out_edge.center_node = center_node;
                        in_edge.center_node = center_node;
                    }
                }
                return true;
            }
        }
        return false;
    }

    pub fn get_num_nodes(&self) -> usize {
        self.num_nodes
    }

    pub fn disconnect(&mut self, node: NodeId) {
        for i in 0..self.out_edges[node].len() {
            let adj = self.out_edges[node][i].adj_node;
            self.remove_in_edge(adj, node);
        }
        for i in 0..self.in_edges[node].len() {
            let adj = self.in_edges[node][i].adj_node;
            self.remove_out_edge(adj, node);
        }
        self.in_edges[node].clear();
        self.out_edges[node].clear();
    }

    pub fn remove_out_edge(&mut self, node: NodeId, adj: NodeId) {
        PreparationGraph::remove_edge_with_adj_node(&mut self.out_edges[node], adj);
    }

    pub fn remove_in_edge(&mut self, node: NodeId, adj: NodeId) {
        PreparationGraph::remove_edge_with_adj_node(&mut self.in_edges[node], adj);
    }

    pub fn remove_edge_with_adj_node(edges: &mut Vec<Arc>, adj: NodeId) {
        let len_before = edges.len();
        edges.retain(|e| e.adj_node != adj);
        assert_eq!(
            edges.len(),
            len_before - 1,
            "should have removed exactly one edge"
        );
    }

    pub fn get_out_edges(&self, node: NodeId) -> &Vec<Arc> {
        return &self.out_edges[node];
    }

    pub fn get_in_edges(&self, node: NodeId) -> &Vec<Arc> {
        return &self.in_edges[node];
    }

    fn assert_valid_node_id(&self, node: NodeId) {
        if node >= self.num_nodes {
            panic!(
                "invalid node id {}, must be in [0, {}]",
                node, self.num_nodes
            );
        }
    }
}

#[derive(Clone)]
pub struct Arc {
    pub adj_node: NodeId,
    pub weight: Weight,
    pub center_node: NodeId,
}

impl Arc {
    pub fn new(adj_node: NodeId, weight: Weight, center_node: NodeId) -> Self {
        Arc {
            adj_node,
            weight,
            center_node,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_edges() {
        let mut g = PreparationGraph::new(4);
        g.add_edge(0, 1, 1);
        g.add_edge(0, 2, 1);
        g.add_edge(0, 3, 1);
        g.add_edge(2, 3, 1);
        assert_eq!(adj_nodes(g.get_out_edges(0)), vec![1, 2, 3]);
        assert_eq!(adj_nodes(g.get_in_edges(3)), vec![0, 2]);

        g.remove_out_edge(0, 2);
        assert_eq!(adj_nodes(g.get_out_edges(0)), vec![1, 3]);
        assert_eq!(adj_nodes(g.get_in_edges(3)), vec![0, 2]);

        g.remove_in_edge(3, 0);
        assert_eq!(adj_nodes(g.get_out_edges(0)), vec![1, 3]);
        assert_eq!(adj_nodes(g.get_in_edges(3)), vec![2]);
    }

    #[test]
    fn add_or_remove_edge() {
        // 0 -> 1
        let mut g = PreparationGraph::new(3);
        g.add_edge(0, 1, 10);
        g.add_or_reduce_edge(0, 1, 6, INVALID_NODE);
        assert_eq!(1, g.get_out_edges(0).len());
        assert_eq!(6, g.get_out_edges(0)[0].weight);
        assert_eq!(1, g.get_in_edges(1).len());
        assert_eq!(6, g.get_in_edges(1)[0].weight);
    }

    #[test]
    fn disconnect() {
        // 0 <-> 1 <-> 2
        let mut g = PreparationGraph::new(4);
        g.add_edge(1, 0, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(0, 1, 1);
        g.add_edge(2, 1, 1);
        assert_eq!(vec![0, 2], adj_nodes(g.get_out_edges(1)));
        assert_eq!(vec![0, 2], adj_nodes(g.get_in_edges(1)));
        g.disconnect(1);
        assert_eq!(0, adj_nodes(g.get_out_edges(0)).len());
        assert_eq!(0, adj_nodes(g.get_out_edges(1)).len());
        assert_eq!(0, adj_nodes(g.get_out_edges(2)).len());
        assert_eq!(0, adj_nodes(g.get_in_edges(0)).len());
        assert_eq!(0, adj_nodes(g.get_in_edges(1)).len());
        assert_eq!(0, adj_nodes(g.get_in_edges(2)).len());
    }

    fn adj_nodes(edges: &Vec<Arc>) -> Vec<NodeId> {
        edges.iter().map(|e| e.adj_node).collect::<Vec<NodeId>>()
    }
}
