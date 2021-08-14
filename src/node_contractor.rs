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
use crate::fast_graph_builder::Params;
use crate::preparation_graph::PreparationGraph;
use crate::witness_search::WitnessSearch;

/// removes all edges incident to `node` from the graph and adds shortcuts between all neighbors
/// of `node` such that all shortest paths are preserved
pub fn contract_node(
    graph: &mut PreparationGraph,
    witness_search: &mut WitnessSearch,
    node: NodeId,
    max_settled_nodes: usize,
) {
    handle_shortcuts(graph, witness_search, node, add_shortcut, max_settled_nodes);
    graph.disconnect(node);
}

pub fn calc_relevance(
    graph: &mut PreparationGraph,
    params: &Params,
    witness_search: &mut WitnessSearch,
    node: NodeId,
    level: NodeId,
    max_settled_nodes: usize,
) -> f32 {
    let mut num_shortcuts = 0;
    handle_shortcuts(
        graph,
        witness_search,
        node,
        |_graph, _shortcut| {
            num_shortcuts += 1;
        },
        max_settled_nodes,
    );
    let num_edges = graph.get_out_edges(node).len() + graph.get_in_edges(node).len();
    let relevance = (params.hierarchy_depth_factor * level as f32)
        + (params.edge_quotient_factor * num_shortcuts as f32 + 1.0) / (num_edges as f32 + 1.0);
    relevance * 1000.0
}

pub fn handle_shortcuts<F>(
    graph: &mut PreparationGraph,
    witness_search: &mut WitnessSearch,
    node: NodeId,
    mut handle_shortcut: F,
    max_settled_nodes: usize,
) where
    F: FnMut(&mut PreparationGraph, Shortcut),
{
    for i in 0..graph.in_edges[node].len() {
        let in_node = graph.in_edges[node][i].adj_node;
        witness_search.init(in_node, node);
        for j in 0..graph.out_edges[node].len() {
            let weight = graph.in_edges[node][i].weight + graph.out_edges[node][j].weight;
            let out_node = graph.out_edges[node][j].adj_node;
            // no need to find the actual weight of a witness path as long as we can be sure
            // that there is some witness with weight smaller or equal to the removed direct
            // path
            let max_witness_weight =
                witness_search.find_max_weight(graph, out_node, weight, max_settled_nodes);
            if max_witness_weight <= weight {
                continue;
            }
            handle_shortcut(graph, Shortcut::new(in_node, out_node, node, weight))
        }
    }
}

fn add_shortcut(graph: &mut PreparationGraph, shortcut: Shortcut) {
    graph.add_or_reduce_edge(
        shortcut.from,
        shortcut.to,
        shortcut.weight,
        shortcut.center_node,
    );
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub struct Shortcut {
    from: NodeId,
    to: NodeId,
    center_node: NodeId,
    weight: Weight,
}

impl Shortcut {
    pub fn new(from: NodeId, to: NodeId, center_node: NodeId, weight: Weight) -> Self {
        Shortcut {
            from,
            to,
            center_node,
            weight,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_contractor;
    use crate::witness_search::WitnessSearch;

    #[test]
    fn calc_shortcuts_no_witness() {
        // 0 -> 2 -> 3
        // 1 ->/ \-> 4
        let mut g = PreparationGraph::new(5);
        g.add_edge(0, 2, 1);
        g.add_edge(1, 2, 2);
        g.add_edge(2, 3, 3);
        g.add_edge(2, 4, 1);
        let shortcuts = calc_shortcuts(&mut g, 2);
        let expected_shortcuts = vec![
            Shortcut::new(0, 3, 2, 4),
            Shortcut::new(0, 4, 2, 2),
            Shortcut::new(1, 3, 2, 5),
            Shortcut::new(1, 4, 2, 3),
        ];
        assert_eq!(expected_shortcuts, shortcuts);
    }

    #[test]
    fn calc_shortcuts_witness() {
        // 0 -> 1 -> 2
        //  \-> 3 ->/
        let mut g = PreparationGraph::new(4);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(0, 3, 1);
        g.add_edge(3, 2, 1);
        let shortcuts = calc_shortcuts(&mut g, 1);
        assert_eq!(0, shortcuts.len());
    }

    #[test]
    fn calc_shortcuts_witness_via_center() {
        // 0 -> 1 -> 2
        // |  /
        // 3 -
        let mut g = PreparationGraph::new(4);
        g.add_edge(0, 1, 10);
        g.add_edge(1, 2, 1);
        g.add_edge(0, 3, 1);
        g.add_edge(3, 1, 1);
        let _shortcuts = calc_shortcuts(&mut g, 1);
        // performance: there is no need for a shortcut 0->1->2, because there is already the
        // (required) shortcut 3->1->2
        let _expected_shortcuts = vec![Shortcut::new(3, 2, 1, 2)];
        // todo: handle this case for better performance (less shortcuts)
        //        assert_eq!(expected_shortcuts, handler.shortcuts);
    }

    #[test]
    fn contract_node() {
        // 0 -> 1 -> 2
        // |  /   \  |
        // 3 --->--- 4
        let mut g = PreparationGraph::new(5);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(0, 3, 1);
        g.add_edge(3, 1, 5);
        g.add_edge(1, 4, 4);
        g.add_edge(3, 4, 3);
        g.add_edge(4, 2, 1);
        let mut witness_search = WitnessSearch::new(g.get_num_nodes());
        node_contractor::contract_node(&mut g, &mut witness_search, 1, usize::MAX);
        // there should be a shortcut 0->2, but no shortcuts 0->4, 3->2
        // node 1 should be properly disconnected
        assert_eq!(0, g.get_out_edges(1).len());
        assert_eq!(0, g.get_in_edges(1).len());
        assert_eq!(2, g.get_out_edges(0).len());
        assert_eq!(2, g.get_in_edges(2).len());
    }

    #[test]
    fn calc_priority() {
        //      3
        //      |
        // 0 -> 1 -> 2 -> 5
        //      |
        //      4
        let mut g = PreparationGraph::new(6);
        g.add_edge(0, 1, 1);
        g.add_edge(1, 2, 1);
        g.add_edge(2, 5, 1);
        g.add_edge(3, 1, 1);
        g.add_edge(1, 4, 1);
        let mut witness_search = WitnessSearch::new(g.get_num_nodes());
        let priorities = vec![
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                0,
                0,
                usize::MAX,
            ),
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                1,
                0,
                usize::MAX,
            ),
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                2,
                0,
                usize::MAX,
            ),
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                3,
                0,
                usize::MAX,
            ),
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                4,
                0,
                usize::MAX,
            ),
            calc_relevance(
                &mut g,
                &Params::default(),
                &mut witness_search,
                5,
                0,
                usize::MAX,
            ),
        ];
        println!("{:?}", priorities);
    }

    fn calc_shortcuts(g: &mut PreparationGraph, node: NodeId) -> Vec<Shortcut> {
        let mut witness_search = WitnessSearch::new(g.get_num_nodes());
        let mut shortcuts = vec![];
        handle_shortcuts(
            g,
            &mut witness_search,
            node,
            |_g, shortcut| shortcuts.push(shortcut),
            usize::MAX,
        );
        shortcuts
    }
}
