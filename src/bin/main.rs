use std::env;

use rand::rngs::StdRng;
use rand::Rng;
use stopwatch::Stopwatch;

use fast_paths::{prepare_with_params, FastGraph, InputGraph, NodeId, Params, PathCalculator};

fn main() {
    // e.g. run like this:
    // cargo run --release main meta/test_maps/graph_ballard.gr
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("You need to pass a file name");
    }
    let filename = &args[2];
    println!(
        "Running performance comparison fast_paths vs. osm_ch for file {}",
        filename
    );

    // read input graph
    let input_graph = InputGraph::from_file(filename);
    println!(
        "number of nodes (input graph) ..... {}",
        input_graph.get_num_nodes()
    );
    println!(
        "number of edges (input graph) ..... {}",
        input_graph.get_num_edges()
    );

    println!("Running preparation and queries for fast_paths");
    let params = Params::default();
    let mut prep_time_fast_paths = Stopwatch::new();
    prep_time_fast_paths.start();
    let fast_graph = prepare_with_params(&input_graph, &params);
    prep_time_fast_paths.stop();
    println!(
        "preparation time .................. {} ms",
        prep_time_fast_paths.elapsed_ms()
    );
    print_fast_graph_stats(&fast_graph);
    let mut path_calculator = PathCalculator::new(fast_graph.get_num_nodes());
    let (checksum_fast_paths, num_not_found_fast_paths) = run_queries(
        &mut |s, t| {
            path_calculator
                .calc_path(&fast_graph, s, t)
                .map(|p| p.get_weight())
        },
        input_graph.get_num_nodes(),
    );

    println!("Running preparation and queries for osm_ch");
    let mut edges = Vec::new();
    let mut max_node = 0;
    for edge in input_graph.get_edges() {
        edges.push(osm_ch_pre::Way::new(edge.from, edge.to, edge.weight));
        max_node = max_node.max(edge.from);
        max_node = max_node.max(edge.to);
    }
    let nodes = (0..=max_node)
        .map(|_| osm_ch_pre::Node { rank: 0 })
        .collect::<Vec<_>>();
    let mut prep_time_fast_paths = Stopwatch::new();
    prep_time_fast_paths.start();
    let osm_ch_output = osm_ch_pre::build_ch(nodes, edges);
    prep_time_fast_paths.stop();
    println!(
        "preparation time .................. {} ms",
        prep_time_fast_paths.elapsed_ms()
    );
    let mut osm_ch_calculator = osm_ch_pre::Calculator::new(osm_ch_output.nodes.len());
    let (checksum_osm_ch, num_not_found_osm_ch) = run_queries(
        &mut |s, t| osm_ch_calculator.query(&osm_ch_output, s, t).map(|r| r.0),
        input_graph.get_num_nodes(),
    );
    println!("checksum fast_paths............... {}", checksum_fast_paths);
    println!(
        "not found fast_paths.............. {}",
        num_not_found_fast_paths
    );
    println!("checksum osm_ch................... {}", checksum_osm_ch);
    println!(
        "not found osm_ch.................. {}",
        num_not_found_osm_ch
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

fn run_queries<F>(calc_path: &mut F, num_nodes: usize) -> (usize, usize)
where
    F: FnMut(NodeId, NodeId) -> Option<usize>,
{
    let num_queries = 100_000;
    let seed = 123;
    let mut rng = create_rng_with_seed(seed);
    let mut checksum = 0;
    let mut num_not_found = 0;
    let mut time = Stopwatch::new();
    for _ in 0..num_queries {
        let source = rng.gen_range(0, num_nodes);
        let target = rng.gen_range(0, num_nodes);
        time.start();
        let weight = calc_path(source, target);
        time.stop();
        match weight {
            Some(weight) => checksum += weight,
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
    (checksum, num_not_found)
}

fn create_rng_with_seed(seed: u64) -> StdRng {
    println!("creating random number generator with seed: {}", seed);
    rand::SeedableRng::seed_from_u64(seed)
}
