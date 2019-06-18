# Fast Paths

The most famous algorithms used to calculate shortest paths are probably Dijkstra's algorithm and A*. However, shortest path calculation can be done much faster by preprocessing the graph.

*Fast Paths* uses *Contraction Hierarchies*, one of the best known speed-up techniques for shortest path calculation. It is especially suited to calculate shortest paths in road networks, but can be used for any directed graph with positive, non-zero edge weights.

### Installation

In `Cargo.toml`

```toml
[dependencies]
fast_paths = "0.1.0"

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
let fast_path_graph = fast_paths::prepare(&input_graph);

// calculate the shortest path between nodes with ID 8 and 6 
let shortest_path = fast_paths::calc_path(&fast_path_graph, 8, 6);

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
let mut path_calculator = fast_paths::create_calculator(&fast_path_graph);
let shortest_path = path_calculator.calc_path(&fast_path_graph, 8, 6);
```

### Saving the prepared graph to disk 

```rust
fast_paths::save_to_disk(&fast_path_graph, "fast_path_graph.fp");
let fast_path_graph = fast_paths::load_from_disk("fast_path_graph.fp");
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

*FastPaths* was run on a single core on a consumer-grade laptop using the road networks provided for the [DIMACS implementation challenge graphs](http://users.diag.uniroma1.it/challenge9/download.shtml). The following graphs were used for the benchmark:

|area|number of nodes|number of edges|
|-|-|-|
|New York|264.346|733.846|
|California&Nevada|1.890.815|4.630.444|
|USA|23.947.347|57.708.624|

|graph|metric|preparation time|average query time (micros)|
|-|-|-|-|
|NY city|distance|24 s|162|
|CAL&NV|distance|100 s|430|
|USA|distance|35 min|3980|
|NY city|time|14 s|77|
|CAL&NV|time|62 s|222|
|USA|time|13 min|1086|

The shortest path calculation time was averaged over 100k random routing queries.
  
### Graph limitations 

- loop-edges (from node A to node A) will be ignored, because since we are only considering positive non-zero edge-weights they cannot be part of a shortest path 
- in case the graph has duplicate edges (multiple edges from node A to node B) only the edge with the lowest weight will be considered

### Special Thanks

Thanks to [Dustin Carlino](http://github.com/dabreegster) from [abstreets](http://github.com/dabreegster/abstreet)!

### License

Apache 2.0
