import graphviz

class Graph:
    def __init__(self, title=None):
        self.title = title if title else "SerializationGraph"
        self.graph = {} # adjacency list
        self.dot = graphviz.Digraph(comment=title)

    def add_edge(self, src, dst, comment=None):
        self.add_node(src)
        self.add_node(dst)

        if dst not in self.graph[src] and src != dst:
            self.graph[src].append(dst)
            self.dot.edge(src, dst, comment)

    def add_node(self, node):
        if node not in self.graph:
            self.graph[node] = []
            self.dot.node(node)

    def find_cycle(self):
        nodes = list(self.graph.keys())
        if len(nodes) == 0:
            print("Error: Empty graph")
            return []

        visited = dict(zip(nodes, [False] * len(nodes)))
        to_visit = [(nodes[0], [])]

        while to_visit:
            (curr, parents) = to_visit.pop()
            
            if curr in parents:
                return parents + [curr]

            if curr in visited and visited[curr]:
                continue

            visited[curr] = True

            neighbors = list(map(
                lambda a: (a, parents + [curr]),
                self.graph[curr]
            ))
            
            for n in neighbors:
                to_visit.append(n)
        return []

    def visualize(self):
        self.dot.render("out/{}".format(self.title), view=False)