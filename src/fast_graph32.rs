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

use std::convert::TryFrom;

use serde::Deserialize;
use serde::Serialize;

use crate::fast_graph::FastGraphEdge;
use crate::FastGraph;

/// Special graph data-structure that is identical to `FastGraph` except that it uses u32 integers
/// instead of usize integers. This is used to store a `FastGraph` in a 32bit representation on disk
/// when using a 64bit system.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FastGraph32 {
    num_nodes: u32,
    pub ranks: Vec<u32>,
    pub edges_fwd: Vec<FastGraphEdge32>,
    pub first_edge_ids_fwd: Vec<u32>,

    pub edges_bwd: Vec<FastGraphEdge32>,
    pub first_edge_ids_bwd: Vec<u32>,
}

impl FastGraph32 {
    /// Creates a 32bit Graph from a given `FastGraph`. All (potentially 64bit) `usize` integers are
    /// simply converted to u32 and if a value exceeds the 32bit limit an error is thrown. The only
    /// exception is `std::u32::MAX`, which is converted to `std::usize::MAX`.
    pub fn new(fast_graph: &FastGraph) -> Self {
        FastGraph32 {
            num_nodes: usize_to_u32(fast_graph.get_num_nodes()),
            ranks: usize_to_u32_vec(&fast_graph.ranks),
            edges_fwd: usize_to_u32_edges(&fast_graph.edges_fwd),
            first_edge_ids_fwd: usize_to_u32_vec(&fast_graph.first_edge_ids_fwd),
            edges_bwd: usize_to_u32_edges(&fast_graph.edges_bwd),
            first_edge_ids_bwd: usize_to_u32_vec(&fast_graph.first_edge_ids_bwd),
        }
    }

    /// Converts a 32bit Graph to an actual `FastGraph` using `usize` such that it can be used with
    /// FastPaths crate. Any integers that equal `std::u32::MAX` are mapped to `std::usize::MAX`.
    pub fn convert_to_usize(self) -> FastGraph {
        let mut g = FastGraph::new(self.num_nodes as usize);
        g.ranks = u32_to_usize_vec(&self.ranks);
        g.edges_fwd = u32_to_usize_edges(&self.edges_fwd);
        g.first_edge_ids_fwd = u32_to_usize_vec(&self.first_edge_ids_fwd);
        g.edges_bwd = u32_to_usize_edges(&self.edges_bwd);
        g.first_edge_ids_bwd = u32_to_usize_vec(&self.first_edge_ids_bwd);
        g
    }
}

/// 32bit equivalent to `FastGraphEdge`, see `FastGraph32` docs.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct FastGraphEdge32 {
    pub base_node: u32,
    pub adj_node: u32,
    pub weight: u32,
    pub replaced_in_edge: u32,
    pub replaced_out_edge: u32,
}

fn usize_to_u32(int: usize) -> u32 {
    if int == std::usize::MAX {
        usize_to_u32(std::u32::MAX as usize)
    } else {
        if let Ok(x) = u32::try_from(int) {
            x
        } else {
            panic!("Could not convert {} to a 32-bit integer", int);
        }
    }
}

fn usize_to_u32_vec(vec: &Vec<usize>) -> Vec<u32> {
    vec.iter().map(|i| usize_to_u32(*i)).collect()
}

fn usize_to_u32_edges(vec: &Vec<FastGraphEdge>) -> Vec<FastGraphEdge32> {
    vec.iter().map(|e| usize_to_u32_edge(e)).collect()
}

fn usize_to_u32_edge(edge: &FastGraphEdge) -> FastGraphEdge32 {
    FastGraphEdge32 {
        base_node: usize_to_u32(edge.base_node),
        adj_node: usize_to_u32(edge.adj_node),
        weight: usize_to_u32(edge.weight),
        replaced_in_edge: usize_to_u32(edge.replaced_in_edge),
        replaced_out_edge: usize_to_u32(edge.replaced_out_edge),
    }
}

fn u32_to_usize(int: u32) -> usize {
    if int == std::u32::MAX {
        std::usize::MAX
    } else {
        int as usize
    }
}

fn u32_to_usize_vec(vec: &Vec<u32>) -> Vec<usize> {
    vec.iter().map(|i| u32_to_usize(*i)).collect()
}

fn u32_to_usize_edges(vec: &Vec<FastGraphEdge32>) -> Vec<FastGraphEdge> {
    vec.iter().map(|e| u32_to_usize_edge(e)).collect()
}

fn u32_to_usize_edge(edge: &FastGraphEdge32) -> FastGraphEdge {
    FastGraphEdge {
        base_node: u32_to_usize(edge.base_node),
        adj_node: u32_to_usize(edge.adj_node),
        weight: u32_to_usize(edge.weight),
        replaced_in_edge: u32_to_usize(edge.replaced_in_edge),
        replaced_out_edge: u32_to_usize(edge.replaced_out_edge),
    }
}

#[cfg(test)]
mod tests {
    use crate::fast_graph::FastGraph;
    use crate::fast_graph::FastGraphEdge;

    use super::*;

    #[test]
    fn create() {
        let num_nodes = 5;
        let ranks = vec![286, 45, 480_001, std::usize::MAX, 4468];
        let edges_fwd = vec![
            FastGraphEdge::new(std::usize::MAX, 598, 48, std::usize::MAX, std::usize::MAX),
            FastGraphEdge::new(
                std::usize::MAX,
                std::usize::MAX,
                std::usize::MAX,
                4,
                std::usize::MAX,
            ),
        ];
        let edges_bwd = vec![FastGraphEdge::new(0, 1, 3, 4, std::usize::MAX)];
        let first_edge_ids_fwd = vec![1, std::usize::MAX, std::usize::MAX];
        let first_edge_ids_bwd = vec![1, std::usize::MAX, 5, std::usize::MAX, 9, 10];

        let mut g = FastGraph::new(num_nodes);
        g.ranks = ranks;
        g.edges_fwd = edges_fwd;
        g.first_edge_ids_fwd = first_edge_ids_fwd;
        g.edges_bwd = edges_bwd;
        g.first_edge_ids_bwd = first_edge_ids_bwd;

        let g32 = FastGraph32::new(&g);
        assert_eq!(g32.num_nodes, 5);

        assert_eq!(g32.ranks.len(), 5);
        assert_eq!(g32.ranks[0], 286);
        assert_eq!(g32.ranks[2], 480_001);
        assert_eq!(g32.ranks[3], std::u32::MAX);

        assert_eq!(g32.edges_fwd.len(), 2);
        assert_eq!(g32.edges_fwd[0].base_node, std::u32::MAX);
        assert_eq!(g32.edges_fwd[0].adj_node, 598);
        assert_eq!(g32.edges_fwd[0].weight, 48);
        assert_eq!(g32.edges_fwd[0].replaced_in_edge, std::u32::MAX);
        assert_eq!(g32.edges_fwd[0].replaced_out_edge, std::u32::MAX);

        assert_eq!(g32.edges_fwd[1].base_node, std::u32::MAX);
        assert_eq!(g32.edges_fwd[1].adj_node, std::u32::MAX);
        assert_eq!(g32.edges_fwd[1].weight, std::u32::MAX);
        assert_eq!(g32.edges_fwd[1].replaced_in_edge, 4);
        assert_eq!(g32.edges_fwd[1].replaced_out_edge, std::u32::MAX);

        assert_eq!(g32.edges_bwd.len(), 1);
        assert_eq!(g32.edges_bwd[0].weight, 3);
        assert_eq!(g32.edges_bwd[0].replaced_out_edge, std::u32::MAX);

        assert_eq!(g32.first_edge_ids_fwd.len(), 3);
        assert_eq!(g32.first_edge_ids_fwd[1], std::u32::MAX);
        assert_eq!(g32.first_edge_ids_bwd.len(), 6);
        assert_eq!(g32.first_edge_ids_bwd[3], std::u32::MAX);
        assert_eq!(g32.first_edge_ids_bwd[4], 9);

        // briefly check back-conversion
        let g_from32 = g32.convert_to_usize();
        assert_eq!(g_from32.get_num_nodes(), 5);
        assert_eq!(
            g_from32.ranks,
            vec![286, 45, 480_001, std::usize::MAX, 4468]
        );
        assert_eq!(g_from32.first_edge_ids_fwd[2], std::usize::MAX);
        assert_eq!(g_from32.first_edge_ids_bwd[0], 1);
        assert_eq!(g_from32.first_edge_ids_bwd[1], std::usize::MAX);
        assert_eq!(g_from32.edges_fwd[0].base_node, std::usize::MAX);
        assert_eq!(g_from32.edges_fwd[0].adj_node, 598);
        assert_eq!(g_from32.edges_fwd[0].weight, 48);
        assert_eq!(g_from32.edges_bwd[0].replaced_in_edge, 4);
    }

    #[test]
    #[should_panic]
    fn create_fails_with_too_large_numbers() {
        let num_nodes = 5;
        let mut g = FastGraph::new(num_nodes);
        g.ranks = vec![5_000_000_000];
        FastGraph32::new(&g);
    }
}
