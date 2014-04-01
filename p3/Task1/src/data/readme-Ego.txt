type	undirected

Dataset statistics
Nodes 	4039
Edges 	88234
Nodes in largest WCC 	4039 (1.000)
Edges in largest WCC 	88234 (1.000)
Nodes in largest SCC 	4039 (1.000)
Edges in largest SCC 	88234 (1.000)
Average clustering coefficient 	0.6055
Number of triangles 	1612010
Fraction of closed triangles 	0.2647
Diameter (longest shortest path) 	8
90-percentile effective diameter 	4.7

Files: 

nodeId.edges : The edges in the ego network for the node 'nodeId'. Edges are undirected for facebook, and directed (a follows b) for twitter and gplus. The 'ego' node does not appear, but it is assumed that they follow every node id that appears in this file.

nodeId.circles : The set of circles for the ego node. Each line contains one circle, consisting of a series of node ids. The first entry in each line is the name of the circle.

nodeId.feat : The features for each of the nodes that appears in the edge file.

nodeId.egofeat : The features for the ego user.

nodeId.featnames : The names of each of the feature dimensions. Features are '1' if the user has this property in their profile, and '0' otherwise. This file has been anonymized for facebook users, since the names of the features would reveal private data.
