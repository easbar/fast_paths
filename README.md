# Fast Paths

The most famous algorithms used to calculate shortest paths are probably Dijkstra's algorithm and A*. However, shortest path calculation can be done much faster by preprocessing the graph.

*Fast Paths* uses *Contraction Hierarchies*, one of the best known speed-up techniques for shortest path calculation. It is especially suited to calculate shortest paths in road networks, but can be used for any directed graph with positive, non-zero edge weights.

### Installation

In `Cargo.toml`

```toml
[dependencies]
fast_paths = "1.0.0"

```
### Basic usage

```rust
// begin with an empty graph
let mut input_graph = InputGraph::new();

// add an edge between nodes with ID 0 and 6, the weight of the edge is 12.
// Note that the node IDs should be consecutive, if your graph has N nodes use 0...N-1 as node IDs,
// otherwise performance will degrade.
input_graph.add_edge(0, 6, 12);
// ... add many more edges here

// freeze the graph before using it (you cannot add more edges afterwards, unless you call thaw() first)
input_graph.freeze();

// prepare the graph for fast shortest path calculations. note that you have to do this again if you want to change the
// graph topology or any of the edge weights
let fast_graph = fast_paths::prepare(&input_graph);

// calculate the shortest path between nodes with ID 8 and 6 
let shortest_path = fast_paths::calc_path(&fast_graph, 8, 6);

match shortest_path {
    Some(p) => {
        // the weight of the shortest path
        let weight = p.get_weight();
        
        // all nodes of the shortest path (including source and target)
        let nodes = p.get_nodes();
    },
    None => {
        // no path has been found (nodes are not connected in this graph)
    }
}


```

### Batch-wise shortest path calculation

For batch-wise calculation of shortest paths the method described above is inefficient. You should keep the `PathCalculator` object to execute multiple queries instead:

```rust
// ... see above
// create a path calculator (note: not thread-safe, use a separate object per thread)
let mut path_calculator = fast_paths::create_calculator(&fast_graph);
let shortest_path = path_calculator.calc_path(&fast_graph, 8, 6);
```

### Calculating paths between multiple sources and targets

We can also efficiently calculate the shortest path when we want to consider multiple sources or targets:

```rust
// ... see above
// we want to either start at node 2 or 3 both of which carry a different initial weight
let sources = vec![(3, 5), (2, 7)];
// ... and go to either node 6 or 8 which also both carry a cost upon arrival
let targets = vec![(6, 2), (8, 10)];
// calculate the path with minimum cost that connects any of the sources with any of the targets while taking into 
// account the initial weights of each source and node
let shortest_path = path_calculator.calc_path_multiple_sources_and_targets(&fast_graph, sources, targets);
```

### Serializing the prepared graph

`FastGraph` implements standard [Serde](https://serde.rs/) serialization.

To be able to use the graph in a 32bit WebAssembly environment, it needs to be transformed to a 32bit representation when preparing it on a 64bit system. This can be achieved with the following two methods, but it will only work for graphs that do not exceed the 32bit limit, i.e. the number of nodes and edges and all weights must be below 2^32.

```rust
use fast_paths::{deserialize_32, serialize_32, FastGraph};

#[derive(Serialize, Deserialize)]
struct YourData {
    #[serde(serialize_with = "serialize_32", deserialize_with = "deserialize_32")]
    graph: FastGraph,
    // the rest of your struct
}
```

### Preparing the graph after changes

The graph preparation can be done much faster using a fixed node ordering, which is just a permutation of node ids. This can be done like this:

```rust
let fast_graph = fast_paths::prepare(&input_graph);
let node_ordering = fast_graph.get_node_ordering();

let another_fast_graph = fast_paths::prepare_with_order(&another_input_graph, &node_ordering);
```

For this to work `another_input_graph` must have the same number of nodes as `input_graph`, otherwise `prepare_with_order` will return an error. Also performance will only be acceptable if `input_graph` and `another_input_graph` are similar to each other, say you only changed a few edge weights. 
 
### Benchmarks

*FastPaths* was run on a single core on a consumer-grade laptop using the road networks provided for the [DIMACS implementation challenge graphs](http://www.diag.uniroma1.it/~challenge9/download.shtml). The following graphs were used for the benchmark:

|area|number of nodes|number of edges|
|-|-|-|
|New York|264.347|730.100|
|California&Nevada|1.890.816|4.630.444|
|USA|23.947.348|57.708.624|

|graph|metric|preparation time|average query time|out edges|in edges|
|-|-|-|-|-|-|
|NY city|distance|9 s|55 μs|747.555|747.559|
|CAL&NV|distance|36 s|103 μs|4.147.109|4.147.183|
|USA|distance|10.6 min|630 μs|52.617.216|52.617.642|
|NY city|time|6 s|26 μs|706.053|706.084|
|CAL&NV|time|24 s|60 μs|3.975.276|3.975.627|
|USA|time|5.5 min|305 μs|49.277.058|49.283.162|

The shortest path calculation time was averaged over 100k random routing queries. The benchmarks were run on a Macbook Pro M1 Max using Rust 1.74.1.
The code for running these benchmarks can be found on the `benchmarks` branch.

There are also some benchmarks using smaller maps included in the test suite. You can run them like this:
```shell
export RUST_TEST_THREADS=1; cargo test --release -- --ignored --nocapture
```

### Graph limitations 

- loop-edges (from node A to node A) will be ignored, because since we are only considering positive non-zero edge-weights they cannot be part of a shortest path 
- in case the graph has duplicate edges (multiple edges from node A to node B) only the edge with the lowest weight will be considered

### Special Thanks

Thanks to [Dustin Carlino](http://github.com/dabreegster) from [A/B Street](http://github.com/dabreegster/abstreet)!


#### License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in fast_paths by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
