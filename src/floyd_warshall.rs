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

use crate::constants::{NodeId, Weight, WEIGHT_MAX};
use crate::input_graph::InputGraph;

pub struct FloydWarshall {
    num_nodes: usize,
    matrix: Vec<Weight>,
}

impl FloydWarshall {
    pub fn new(num_nodes: usize) -> Self {
        // todo: move num_nodes initialization into prepare and prevent calling calc_path before
        // prepare
        FloydWarshall {
            num_nodes,
            matrix: vec![WEIGHT_MAX; num_nodes * num_nodes],
        }
    }

    pub fn prepare(&mut self, input_graph: &InputGraph) {
        assert_eq!(
            input_graph.get_num_nodes(),
            self.num_nodes,
            "input graph has invalid number of nodes"
        );
        let n = self.num_nodes;
        for e in input_graph.get_edges() {
            self.matrix[e.from * n + e.to] = e.weight;
        }
        for k in 0..n {
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        self.matrix[i * n + j] = 0;
                    }
                    let weight_ik = self.matrix[i * n + k];
                    let weight_kj = self.matrix[k * n + j];
                    if weight_ik == WEIGHT_MAX || weight_kj == WEIGHT_MAX {
                        continue;
                    }
                    let idx = i * n + j;
                    self.matrix[idx] = cmp::min(self.matrix[idx], weight_ik + weight_kj)
                }
            }
        }
    }

    pub fn calc_weight(&self, source: NodeId, target: NodeId) -> Weight {
        return self.matrix[source * self.num_nodes + target];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calc_weights() {
        // 0 -> 1 -- 3
        // |         |
        // 4 -> 5 -> 6
        //      |    |
        //      7 -> 8
        let mut g = InputGraph::new();
        g.add_edge(0, 1, 6);
        g.add_edge(0, 4, 1);
        g.add_edge(4, 5, 1);
        g.add_edge(5, 7, 1);
        g.add_edge(7, 8, 1);
        g.add_edge(8, 6, 1);
        g.add_edge(6, 3, 1);
        g.add_edge(3, 1, 1);
        g.add_edge(1, 3, 1);
        g.add_edge(5, 6, 4);
        g.freeze();
        let mut fw = FloydWarshall::new(g.get_num_nodes());
        fw.prepare(&g);
        assert_eq!(fw.calc_weight(0, 3), 6);
        assert_eq!(fw.calc_weight(5, 3), 4);
        assert_eq!(fw.calc_weight(1, 1), 0);
        assert_eq!(fw.calc_weight(5, 5), 0);
        assert_eq!(fw.calc_weight(6, 5), WEIGHT_MAX);
        assert_eq!(fw.calc_weight(8, 0), WEIGHT_MAX);
    }
}
