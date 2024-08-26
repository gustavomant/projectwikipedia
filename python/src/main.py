from graph_metrics import GraphMetrics
from create_graph_from_parquet import create_graph_from_parquet

graph = create_graph_from_parquet("../assets/processed-all-refs.parquet")
graph_metrics = GraphMetrics(graph)

print(f"Número de vértices: {graph_metrics.num_vertices()}")
print(f"Número de arestas: {graph_metrics.num_edges()}")
print(f"Grau médio: {graph_metrics.average_degree()}")
print(f"Grau de entrada médio: {graph_metrics.average_in_degree()}")
print(f"Grau de saída médio: {graph_metrics.average_out_degree()}")
print(f"Diâmetro: {graph_metrics.diameter()}")
print(f"Densidade: {graph_metrics.density()}")
print(f"Coeficiente de agrupamento: {graph_metrics.clustering_coefficient()}")
