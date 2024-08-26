class GraphMetrics:
    def __init__(self, graph):
        self.graph = graph

    def num_vertices(self):
        return len(self.graph.vs)

    def num_edges(self):
        return len(self.graph.es)

    def average_degree(self):
        return sum(self.graph.degree()) / len(self.graph.vs)

    def average_in_degree(self):
        return sum(self.graph.indegree()) / len(self.graph.vs)

    def average_out_degree(self):
        return sum(self.graph.outdegree()) / len(self.graph.vs)

    def diameter(self):
        if not self.graph.is_connected():
            largest_component = self.graph.clusters().giant()
            diameter = largest_component.diameter()
        else:
            diameter = self.graph.diameter()

        return diameter

    def density(self):
        num_vertices = len(self.graph.vs)
        num_edges = len(self.graph.es)
        # For a directed graph
        return num_edges / (num_vertices * (num_vertices - 1))

    def clustering_coefficient(self):
        # For a directed graph
        local_clustering_coeffs = [self.graph.transitivity_local_undirected(v) for v in self.graph.vs]
        return sum(local_clustering_coeffs) / len(local_clustering_coeffs)