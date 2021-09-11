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

use std::cmp;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[cfg(test)]
use rand::rngs::StdRng;
#[cfg(test)]
use rand::Rng;

use serde::{Deserialize, Serialize};

use crate::constants::NodeId;
use crate::constants::Weight;

#[derive(Serialize, Deserialize, Clone)]
pub struct InputGraph {
    edges: Vec<Edge>,
    num_nodes: usize,
    frozen: bool,
}

impl InputGraph {
    pub fn new() -> Self {
        InputGraph {
            edges: Vec::new(),
            num_nodes: 0,
            frozen: false,
        }
    }

    #[cfg(test)]
    pub fn random(rng: &mut StdRng, num_nodes: usize, mean_degree: f32) -> Self {
        InputGraph::build_random_graph(rng, num_nodes, mean_degree)
    }

    pub fn from_file(filename: &str) -> Self {
        InputGraph::read_from_file(filename)
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId, weight: Weight) -> usize {
        self.do_add_edge(from, to, weight, false)
    }

    pub fn add_edge_bidir(&mut self, from: NodeId, to: NodeId, weight: Weight) -> usize {
        self.do_add_edge(from, to, weight, true)
    }

    pub fn get_edges(&self) -> &Vec<Edge> {
        self.check_frozen();
        &self.edges
    }

    pub fn get_num_nodes(&self) -> usize {
        self.check_frozen();
        self.num_nodes
    }

    pub fn get_num_edges(&self) -> usize {
        self.check_frozen();
        self.edges.len()
    }

    pub fn freeze(&mut self) {
        if self.frozen {
            panic!("Input graph is already frozen");
        }
        self.sort();
        self.remove_duplicate_edges();
        self.frozen = true;
    }

    pub fn thaw(&mut self) {
        self.frozen = false;
    }

    fn sort(&mut self) {
        self.edges.sort_by(|a, b| {
            a.from
                .cmp(&b.from)
                .then(a.to.cmp(&b.to))
                .then(a.weight.cmp(&&b.weight))
        });
    }

    fn remove_duplicate_edges(&mut self) {
        // we go through (already sorted!) list of edges and remove duplicates
        let len_before = self.edges.len();
        self.edges.dedup_by(|a, b| a.from == b.from && a.to == b.to);
        if len_before != self.edges.len() {
            warn!(
                "There were {} duplicate edges, only the ones with lowest weight were kept",
                len_before - self.edges.len()
            );
        }
    }

    pub fn unit_test_output_string(&self) -> String {
        return self
            .edges
            .iter()
            .map(|e| e.unit_test_output_string())
            .collect::<Vec<String>>()
            .join("\n")
            + "\n";
    }

    fn check_frozen(&self) {
        if !self.frozen {
            panic!("You need to call freeze() before using the input graph")
        }
    }

    fn do_add_edge(&mut self, from: NodeId, to: NodeId, weight: Weight, bidir: bool) -> usize {
        if self.frozen {
            panic!("Graph is frozen already, for further changes first use thaw()");
        }
        if from == to {
            warn!(
                "Loop edges are not allowed. Skipped edge! from: {}, to: {}, weight: {}",
                from, to, weight
            );
            return 0;
        }
        if weight < 1 {
            warn!(
                "Zero weight edges are not allowed. Skipped edge! from: {}, to: {}, weight: {}",
                from, to, weight
            );
            return 0;
        }
        self.num_nodes = cmp::max(self.num_nodes, cmp::max(from, to) + 1);
        self.edges.push(Edge::new(from, to, weight));
        if bidir {
            self.edges.push(Edge::new(to, from, weight));
        }
        if bidir {
            2
        } else {
            1
        }
    }

    /// Builds a random graph, mostly used for testing purposes
    #[cfg(test)]
    fn build_random_graph(rng: &mut StdRng, num_nodes: usize, mean_degree: f32) -> InputGraph {
        let num_edges = (mean_degree * num_nodes as f32) as usize;
        let mut result = InputGraph::new();
        let mut edge_count = 0;
        loop {
            let head = rng.gen_range(0, num_nodes);
            let tail = rng.gen_range(0, num_nodes);
            // limit max weight, but otherwise allow duplicates, loops etc. to make sure clean-up
            // inside InputGraph works correctly
            let weight = rng.gen_range(1, 100);
            edge_count += result.add_edge(tail, head, weight);
            if edge_count == num_edges {
                break;
            }
        }
        result.freeze();
        result
    }

    /// Reads input graph from a text file, using the following format:
    /// a <from> <to> <weight>
    /// Mostly used for performance testing.
    fn read_from_file(filename: &str) -> Self {
        let file = File::open(filename).unwrap();
        let reader = BufReader::new(file);
        let mut g = InputGraph::new();
        for line in reader.lines() {
            let s: String = line.unwrap();
            if !s.starts_with('a') {
                continue;
            } else {
                let mut split = s[2..].split_whitespace();
                let from = split.next().unwrap().parse::<usize>().unwrap();
                let to = split.next().unwrap().parse::<usize>().unwrap();
                let weight = split.next().unwrap().parse::<usize>().unwrap();
                assert!(split.next().is_none(), "Invalid arc line: {}", s);
                g.add_edge(from, to, weight);
            }
        }
        g.freeze();
        g
    }
}

impl fmt::Debug for InputGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.unit_test_output_string())
    }
}

impl Default for InputGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub weight: Weight,
}

impl Edge {
    pub fn new(from: NodeId, to: NodeId, weight: Weight) -> Edge {
        Edge { from, to, weight }
    }

    pub fn unit_test_output_string(&self) -> String {
        return format!("g.add_edge({}, {}, {});", self.from, self.to, self.weight);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn panic_if_not_frozen_get_edges() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 3);
        g.get_edges();
    }

    #[test]
    #[should_panic]
    fn panic_if_not_frozen_get_num_edges() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 3);
        g.get_num_edges();
    }

    #[test]
    #[should_panic]
    fn panic_if_not_frozen_get_num_nodes() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 3);
        g.get_num_nodes();
    }

    #[test]
    #[should_panic]
    fn panic_if_frozen_add_edge() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 3);
        g.freeze();
        g.add_edge(2, 5, 4);
    }

    #[test]
    fn freeze_and_thaw() {
        let mut g = InputGraph::new();
        g.add_edge(0, 5, 10);
        g.add_edge(0, 5, 5);
        g.freeze();
        assert_eq!(1, g.get_num_edges());
        g.thaw();
        g.add_edge(0, 5, 1);
        g.freeze();
        assert_eq!(1, g.get_num_edges());
        assert_eq!(1, g.get_edges()[0].weight);
    }

    #[test]
    fn num_nodes() {
        let mut g = InputGraph::new();
        g.add_edge(7, 1, 2);
        g.add_edge(5, 6, 4);
        g.add_edge(11, 8, 3);
        g.freeze();
        assert_eq!(12, g.get_num_nodes());
    }

    #[test]
    fn skips_loops() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 3);
        g.add_edge(4, 4, 2);
        g.add_edge(2, 5, 4);
        g.freeze();
        assert_eq!(2, g.get_num_edges());
    }

    #[test]
    fn skips_zero_weight_edges() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 5);
        g.add_edge(1, 2, 0);
        g.add_edge(2, 3, 3);
        g.freeze();
        assert_eq!(2, g.get_num_edges());
    }

    #[test]
    fn skips_duplicate_edges() {
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 7);
        g.add_edge(2, 3, 5);
        g.add_edge(0, 2, 3);
        g.add_edge(0, 1, 2);
        g.add_edge(4, 6, 9);
        g.add_edge(0, 1, 4);
        g.freeze();
        assert_eq!(4, g.get_num_edges());
        // edges should be sorted and duplicates should be removed keeping only the ones with
        // lowest weight
        let weights = g
            .get_edges()
            .iter()
            .map(|e| e.weight)
            .collect::<Vec<Weight>>();
        assert_eq!(vec![2, 3, 5, 9], weights);
    }

    #[test]
    fn skips_duplicate_edges_more() {
        let mut g = InputGraph::new();
        g.add_edge(1, 3, 43);
        g.add_edge(3, 2, 90);
        g.add_edge(3, 2, 88);
        g.add_edge(2, 3, 87);
        g.add_edge(3, 0, 75);
        g.add_edge(0, 2, 45);
        g.add_edge(1, 3, 71);
        g.add_edge(4, 3, 5);
        g.add_edge(1, 3, 91);
        g.freeze();
        assert_eq!(6, g.get_num_edges());
        let weights = g
            .get_edges()
            .iter()
            .map(|e| e.weight)
            .collect::<Vec<Weight>>();
        assert_eq!(vec![45, 43, 87, 75, 88, 5], weights);
    }
}
