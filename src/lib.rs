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

#[macro_use]
extern crate log;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use crate::constants::*;
pub use crate::fast_graph::FastGraph;
pub use crate::fast_graph32::FastGraph32;
pub use crate::fast_graph_builder::FastGraphBuilder;
pub use crate::fast_graph_builder::Params;
pub use crate::input_graph::Edge;
pub use crate::input_graph::InputGraph;
pub use crate::path_calculator::PathCalculator;
pub use crate::shortest_path::ShortestPath;

mod constants;
mod dijkstra;
mod fast_graph;
mod fast_graph32;
mod fast_graph_builder;
#[cfg(test)]
mod floyd_warshall;
mod heap_item;
mod input_graph;
mod node_contractor;
mod path_calculator;
mod preparation_graph;
mod shortest_path;
mod valid_flags;

/// Prepares the given `InputGraph` for fast shortest path calculations.
pub fn prepare(input_graph: &InputGraph) -> FastGraph {
    FastGraphBuilder::build(input_graph)
}

/// Like `prepare()`, but allows specifying some parameters used for the graph preparation.
pub fn prepare_with_params(input_graph: &InputGraph, params: &Params) -> FastGraph {
    FastGraphBuilder::build_with_params(input_graph, params)
}

/// Prepares the given input graph using a fixed node ordering, which can be any permutation
/// of the node ids. This can be used to speed up the graph preparation if you have done
/// it for a similar graph with an equal number of nodes. For example if you have changed some
/// of the edge weights only.
pub fn prepare_with_order(
    input_graph: &InputGraph,
    order: &Vec<NodeId>,
) -> Result<FastGraph, String> {
    FastGraphBuilder::build_with_order(input_graph, order)
}

/// Calculates the shortest path from `source` to `target`.
pub fn calc_path(fast_graph: &FastGraph, source: NodeId, target: NodeId) -> Option<ShortestPath> {
    let mut calc = PathCalculator::new(fast_graph.get_num_nodes());
    calc.calc_path(fast_graph, source, target)
}

/// Calculates the shortest path from any of the `sources` to a single `target`.
///
/// The path returned will start at the source node that's closest to `target`. An additional
/// weight for each source can be specified.
///
/// TODO: Support multiple targets.
pub fn calc_path_multiple_endpoints(
    fast_graph: &FastGraph,
    sources: Vec<(NodeId, Weight)>,
    target: NodeId,
) -> Option<ShortestPath> {
    let mut calc = PathCalculator::new(fast_graph.get_num_nodes());
    calc.calc_path_multiple_endpoints(fast_graph, sources, target)
}

/// Creates a `PathCalculator` that can be used to run many shortest path calculations in a row.
/// This is the preferred way to calculate shortest paths in case you are calculating more than
/// one path. Use one `PathCalculator` for each thread.
pub fn create_calculator(fast_graph: &FastGraph) -> PathCalculator {
    PathCalculator::new(fast_graph.get_num_nodes())
}

/// Returns the node ordering of a prepared graph. This can be used to run the preparation with
/// `prepare_with_order()`.
pub fn get_node_ordering(fast_graph: &FastGraph) -> Vec<NodeId> {
    fast_graph.get_node_ordering()
}

/// When serializing a `FastGraph` in a larger struct, use `#[serde(serialize_with =
/// "fast_paths::serialize_32`)]` to transform the graph to a 32-bit representation. This will use
/// 50% more RAM than serializing without transformation, but the resulting size will be 50% less.
/// It will panic if the graph has more than 2^32 nodes or edges or values for weight.
pub fn serialize_32<S: Serializer>(fg: &FastGraph, s: S) -> Result<S::Ok, S::Error> {
    FastGraph32::new(fg).serialize(s)
}

/// When deserializing a `FastGraph` in a larger struct, use `#[serde(deserialize_with =
/// "fast_paths::deserialize_32`)]` to transform the graph from a 32-bit representation to the
/// current platform's supported size. This is necessary when serializing on a 64-bit system and
/// deserializing on a 32-bit system, such as WASM.
pub fn deserialize_32<'de, D: Deserializer<'de>>(d: D) -> Result<FastGraph, D::Error> {
    let fg32 = <FastGraph32>::deserialize(d)?;
    Ok(fg32.convert_to_usize())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs::{remove_file, File};
    use std::time::SystemTime;

    use rand::rngs::StdRng;
    use rand::Rng;
    use stopwatch::Stopwatch;

    use crate::constants::NodeId;
    use crate::dijkstra::Dijkstra;
    use crate::fast_graph::FastGraph;
    use crate::floyd_warshall::FloydWarshall;
    use crate::path_calculator::PathCalculator;
    use crate::preparation_graph::PreparationGraph;

    use super::*;

    #[test]
    fn routing_on_random_graph() {
        const REPEATS: usize = 100;
        for _i in 0..REPEATS {
            run_test_on_random_graph();
        }
    }

    fn run_test_on_random_graph() {
        const NUM_NODES: usize = 50;
        const NUM_QUERIES: usize = 1_000;
        const MEAN_DEGREE: f32 = 2.0;

        let mut rng = create_rng();
        let input_graph = InputGraph::random(&mut rng, NUM_NODES, MEAN_DEGREE);
        debug!("random graph: \n {:?}", input_graph);
        let fast_graph = prepare(&input_graph);
        let mut path_calculator = create_calculator(&fast_graph);

        let dijkstra_graph = PreparationGraph::from_input_graph(&input_graph);
        let mut dijkstra = Dijkstra::new(input_graph.get_num_nodes());

        let mut fw = FloydWarshall::new(input_graph.get_num_nodes());
        fw.prepare(&input_graph);

        let mut num_different_paths = 0;
        for _i in 0..NUM_QUERIES {
            let source = rng.gen_range(0, input_graph.get_num_nodes());
            let target = rng.gen_range(0, input_graph.get_num_nodes());
            let path_fast = path_calculator
                .calc_path(&fast_graph, source, target)
                .unwrap_or(ShortestPath::none(source, target));
            let path_dijkstra = dijkstra
                .calc_path(&dijkstra_graph, source, target)
                .unwrap_or(ShortestPath::none(source, target));
            let weight_fast = path_fast.get_weight();
            let weight_dijkstra = path_dijkstra.get_weight();
            let weight_fw = fw.calc_weight(source, target);
            assert_eq!(
                weight_fw, weight_fast,
                "\nNo agreement for routing query from: {} to: {}\nFloyd-Warshall: {}\nCH: {}\
                 \n Failing graph:\n{:?}",
                source, target, weight_fw, weight_fast, input_graph
            );
            assert_eq!(
                path_dijkstra, path_fast,
                "\nNo agreement for routing query from: {} to: {}\nDijkstra: {}\nCH: {}\
                 \n Failing graph:\n{:?}",
                source, target, weight_dijkstra, weight_fast, input_graph
            );
            if path_dijkstra.get_nodes() != path_fast.get_nodes() {
                num_different_paths += 1;
            }
        }
        if num_different_paths as f32 > 0.1 * NUM_QUERIES as f32 {
            panic!(
                "too many different paths: {}, out of {}, a few different paths can be expected \
                    because of unambiguous shortest paths, but if there are too many something is \
                    wrong",
                num_different_paths, NUM_QUERIES
            );
        }
    }

    #[test]
    fn multi_source_routing_on_random_graph() {
        const REPEATS: usize = 100;
        for _ in 0..REPEATS {
            const NUM_NODES: usize = 50;
            const NUM_QUERIES: usize = 1_000;
            const MEAN_DEGREE: f32 = 2.0;
            const NUM_SOURCES: usize = 3;

            let mut rng = create_rng();
            let input_graph = InputGraph::random(&mut rng, NUM_NODES, MEAN_DEGREE);
            debug!("random graph: \n {:?}", input_graph);
            let fast_graph = prepare(&input_graph);
            let mut path_calculator = create_calculator(&fast_graph);

            let dijkstra_graph = PreparationGraph::from_input_graph(&input_graph);
            let mut dijkstra = Dijkstra::new(input_graph.get_num_nodes());

            let mut num_different_paths = 0;
            for _ in 0..NUM_QUERIES {
                // This may pick duplicate source nodes, and even duplicate source nodes with
                // different weights; anyway that shouldn't break anything.
                let sources: Vec<(NodeId, Weight)> = (0..NUM_SOURCES)
                    .map(|_| {
                        (
                            rng.gen_range(0, input_graph.get_num_nodes()),
                            // sometimes use sources nodes with max weight
                            if rng.gen_range(0, 100) < 3 {
                                WEIGHT_MAX
                            } else {
                                rng.gen_range(0, 100)
                            },
                        )
                    })
                    .collect();
                let target = rng.gen_range(0, input_graph.get_num_nodes());
                let fast_path = path_calculator.calc_path_multiple_endpoints(
                    &fast_graph,
                    sources.clone(),
                    target,
                );
                let dijkstra_paths: Vec<(Option<ShortestPath>, Weight)> = sources
                    .iter()
                    .map(|(source, weight)| {
                        (
                            dijkstra.calc_path(&dijkstra_graph, *source, target),
                            *weight,
                        )
                    })
                    .collect();

                let found_dijkstras: Vec<(ShortestPath, usize)> = dijkstra_paths
                    .into_iter()
                    .filter(|(p, w)| p.is_some() && *w < WEIGHT_MAX)
                    .map(|(p, w)| (p.unwrap(), w))
                    .collect();

                // We have to make sure fast_path is as short as the shortest of all dijkstra_paths
                let shortest_dijkstra_weight = found_dijkstras
                    .iter()
                    .map(|(p, w)| p.get_weight() + w)
                    .min();

                if shortest_dijkstra_weight.is_none() {
                    assert!(fast_path.is_none());
                    return;
                }

                assert!(fast_path.is_some());
                let f = fast_path.unwrap();
                let w = shortest_dijkstra_weight.unwrap();
                assert_eq!(w, f.get_weight(),
                           "\nfast_path's weight {} does not match the weight of the shortest Dijkstra path {}.\
                               \nsources: {:?}\
                               \ntargets: {}\
                               \nFailing graph:\n{:?}",
                           f.get_weight(), w, sources, target, input_graph);

                // There can be multiple options with the same weight. fast_path has to match
                // at least one of them
                let matching_dijkstras: Vec<(ShortestPath, Weight)> = found_dijkstras
                    .into_iter()
                    .filter(|(p, w)| {
                        p.get_weight() + w == f.get_weight()
                            && p.get_source() == f.get_source()
                            && p.get_target() == f.get_target()
                    })
                    .collect();

                assert!(
                    matching_dijkstras.len() > 0,
                    "There has to be at least one Dijkstra path with source,target and weight equal to fast_path"
                );

                // one of the matching Dijkstra's should have the same nodes as fast_path, but in
                // some rare cases this can be the case
                if !matching_dijkstras
                    .into_iter()
                    .any(|(p, _)| p.get_nodes() == f.get_nodes())
                {
                    num_different_paths += 1;
                }
            }
            if num_different_paths as f32 > 0.1 * NUM_QUERIES as f32 {
                panic!(
                    "too many different paths: {}, out of {}, a few different paths can be expected \
                        because of unambiguous shortest paths, but if there are too many something is \
                        wrong",
                    num_different_paths, NUM_QUERIES
                );
            }
        }
    }

    #[test]
    fn save_to_and_load_from_disk() {
        let mut g = InputGraph::new();
        g.add_edge(0, 5, 6);
        g.add_edge(5, 2, 1);
        g.add_edge(2, 3, 4);
        g.freeze();
        let fast_graph = prepare(&g);
        save_to_disk(&fast_graph, "example.fp").expect("writing to disk failed");
        let loaded = load_from_disk("example.fp").unwrap();
        remove_file("example.fp").expect("deleting file failed");
        assert_eq!(fast_graph.get_num_nodes(), loaded.get_num_nodes());
        assert_eq!(fast_graph.get_num_in_edges(), loaded.get_num_in_edges());
        assert_eq!(fast_graph.get_num_out_edges(), loaded.get_num_out_edges());
    }

    #[test]
    fn save_to_and_load_from_disk_32() {
        let mut g = InputGraph::new();
        g.add_edge(0, 5, 6);
        g.add_edge(5, 2, 1);
        g.add_edge(2, 3, 4);
        g.freeze();
        let fast_graph = prepare(&g);
        save_to_disk32(&fast_graph, "example32.fp").expect("writing to disk failed");
        let loaded = load_from_disk32("example32.fp").unwrap();
        remove_file("example32.fp").expect("deleting file failed");
        assert_eq!(fast_graph.get_num_nodes(), loaded.get_num_nodes());
        assert_eq!(fast_graph.get_num_in_edges(), loaded.get_num_in_edges());
        assert_eq!(fast_graph.get_num_out_edges(), loaded.get_num_out_edges());
    }

    #[test]
    fn deterministic_result() {
        const NUM_NODES: usize = 50;
        const MEAN_DEGREE: f32 = 2.0;

        // Repeat a few times to reduce test flakiness.
        for _ in 0..10 {
            let mut rng = create_rng();
            let input_graph = InputGraph::random(&mut rng, NUM_NODES, MEAN_DEGREE);
            let serialized1 = bincode::serialize(&prepare(&input_graph)).unwrap();
            let serialized2 = bincode::serialize(&prepare(&input_graph)).unwrap();
            if serialized1 != serialized2 {
                panic!("Preparing and serializing the same graph twice produced different results");
            }
        }
    }

    #[ignore]
    #[test]
    fn run_performance_test_dist() {
        println!("Running performance test for Bremen dist");
        // road network extracted from OSM data from Bremen, Germany using the road distance as weight
        run_performance_test(
            &InputGraph::from_file("meta/test_maps/bremen_dist.gr"),
            &Params::default(),
            845493338,
            30265,
        )
    }

    #[ignore]
    #[test]
    fn run_performance_test_time() {
        println!("Running performance test for Bremen time");
        // road network extracted from OSM data from Bremen, Germany using the travel time as weight
        run_performance_test(
            &InputGraph::from_file("meta/test_maps/bremen_time.gr"),
            &Params::default(),
            88104267255,
            30265,
        );
    }

    #[ignore]
    #[test]
    fn run_performance_test_ballard() {
        println!("Running performance test for ballard");
        run_performance_test(
            &InputGraph::from_file("meta/test_maps/graph_ballard.gr"),
            &Params::new(0.01),
            28409159409,
            14992,
        );
    }

    #[ignore]
    #[test]
    fn run_performance_test_23rd() {
        println!("Running performance test for 23rd");
        run_performance_test(
            &InputGraph::from_file("meta/test_maps/graph_23rd.gr"),
            &Params::default(),
            19438403873,
            20421,
        );
    }

    #[ignore]
    #[test]
    fn run_performance_test_dist_fixed_ordering() {
        println!("Running performance test for Bremen dist (fixed node ordering)");
        let input_graph = InputGraph::from_file("meta/test_maps/bremen_dist.gr");
        let mut fast_graph = prepare(&input_graph);
        let order = get_node_ordering(&fast_graph);
        prepare_algo(
            &mut |input_graph| fast_graph = prepare_with_order(input_graph, &order).unwrap(),
            &input_graph,
        );
        print_fast_graph_stats(&fast_graph);
        let mut path_calculator = PathCalculator::new(fast_graph.get_num_nodes());
        do_run_performance_test(
            &mut |s, t| path_calculator.calc_path(&fast_graph, s, t),
            input_graph.get_num_nodes(),
            845493338,
            30265,
        );
    }

    fn run_performance_test(
        input_graph: &InputGraph,
        params: &Params,
        expected_checksum: usize,
        expected_num_not_found: usize,
    ) {
        let mut fast_graph = FastGraph::new(1);
        prepare_algo(
            &mut |input_graph| fast_graph = prepare_with_params(input_graph, params),
            &input_graph,
        );
        print_fast_graph_stats(&fast_graph);
        let mut path_calculator = PathCalculator::new(fast_graph.get_num_nodes());
        do_run_performance_test(
            &mut |s, t| path_calculator.calc_path(&fast_graph, s, t),
            input_graph.get_num_nodes(),
            expected_checksum,
            expected_num_not_found,
        );
    }

    fn print_fast_graph_stats(fast_graph: &FastGraph) {
        println!(
            "number of nodes (fast graph) ...... {}",
            fast_graph.get_num_nodes()
        );
        println!(
            "number of out-edges (fast graph) .. {}",
            fast_graph.get_num_out_edges()
        );
        println!(
            "number of in-edges  (fast graph) .. {}",
            fast_graph.get_num_in_edges()
        );
    }

    pub fn prepare_algo<F>(preparation: &mut F, input_graph: &InputGraph)
    where
        F: FnMut(&InputGraph),
    {
        let mut time = Stopwatch::new();
        time.start();
        preparation(&input_graph);
        time.stop();
        println!(
            "number of nodes (input graph) ..... {}",
            input_graph.get_num_nodes()
        );
        println!(
            "number of edges (input graph) ..... {}",
            input_graph.get_num_edges()
        );
        println!(
            "preparation time .................. {} ms",
            time.elapsed_ms()
        );
    }

    fn do_run_performance_test<F>(
        calc_path: &mut F,
        num_nodes: usize,
        expected_checksum: usize,
        expected_num_not_found: usize,
    ) where
        F: FnMut(NodeId, NodeId) -> Option<ShortestPath>,
    {
        let num_queries = 100_000;
        let seed = 123;
        let mut rng = create_rng_with_seed(seed);
        let mut checksum = 0;
        let mut num_not_found = 0;
        let mut time = Stopwatch::new();
        for _i in 0..num_queries {
            let source = rng.gen_range(0, num_nodes);
            let target = rng.gen_range(0, num_nodes);
            time.start();
            let path = calc_path(source, target);
            time.stop();
            match path {
                Some(path) => checksum += path.get_weight(),
                None => num_not_found += 1,
            }
        }
        println!(
            "total query time .................. {} ms",
            time.elapsed_ms()
        );
        println!(
            "query time on average ............. {} micros",
            time.elapsed().as_micros() / (num_queries as u128)
        );
        assert_eq!(expected_checksum, checksum, "invalid checksum");
        assert_eq!(
            expected_num_not_found, num_not_found,
            "invalid number of paths not found"
        );
    }

    fn create_rng() -> StdRng {
        let seed = create_seed();
        create_rng_with_seed(seed)
    }

    fn create_rng_with_seed(seed: u64) -> StdRng {
        debug!("creating random number generator with seed: {}", seed);
        rand::SeedableRng::seed_from_u64(seed)
    }

    fn create_seed() -> u64 {
        SystemTime::now().elapsed().unwrap().as_nanos() as u64
    }

    /// Saves the given prepared graph to disk
    fn save_to_disk(fast_graph: &FastGraph, file_name: &str) -> Result<(), Box<dyn Error>> {
        let file = File::create(file_name)?;
        Ok(bincode::serialize_into(file, fast_graph)?)
    }

    /// Restores a prepared graph from disk
    fn load_from_disk(file_name: &str) -> Result<FastGraph, Box<dyn Error>> {
        let file = File::open(file_name)?;
        Ok(bincode::deserialize_from(file)?)
    }

    /// Saves the given prepared graph to disk thereby enforcing a 32bit representation no matter whether
    /// the system in use uses 32 or 64bit. This is useful when creating the graph on a 64bit system and
    /// afterwards loading it on a 32bit system.
    /// Note: Using this method requires an extra +50% of RAM while storing the graph (even though
    /// the graph will use 50% *less* disk space when it has been saved.
    fn save_to_disk32(fast_graph: &FastGraph, file_name: &str) -> Result<(), Box<dyn Error>> {
        let fast_graph32 = &FastGraph32::new(fast_graph);
        let file = File::create(file_name)?;
        Ok(bincode::serialize_into(file, fast_graph32)?)
    }

    /// Loads a graph from disk that was saved in 32bit representation, i.e. using save_to_disk32. The
    /// graph will use usize to store integers, so most commonly either 32 or 64bits per integer
    /// depending on the system in use.
    /// Note: Using this method requires an extra +50% RAM while loading the graph.
    fn load_from_disk32(file_name: &str) -> Result<FastGraph, Box<dyn Error>> {
        let file = File::open(file_name)?;
        let r: Result<FastGraph32, Box<dyn Error>> = Ok(bincode::deserialize_from(file)?);
        r.map(|g| g.convert_to_usize())
    }
}
