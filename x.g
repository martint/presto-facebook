digraph G {
	ranksep=1.5;
	compound=true;
	node [shape=rectangle];	subgraph cluster_1{
		label="1";
		{rank=same; 12 1 7}
		12 [label="TableExpression (12)",fillcolor=salmon,style=filled];
		1 [label="TableExpression (1)",fillcolor=lightblue,style=filled];
		7 [label="TableExpression (7)",fillcolor=salmon,style=filled];
	}
	subgraph cluster_2{
		label="2";
		{rank=same; 9 2}
		9 [label="ProjectExpression (9)",fillcolor=salmon,style=filled];
		2 [label="ProjectExpression (2)",fillcolor=lightblue,style=filled];
	}
	subgraph cluster_3{
		label="3";
		{rank=same; 16 3 11 5}
		16 [label="ProjectExpression (16)",fillcolor=salmon,style=filled];
		3 [label="FilterExpression (3)",fillcolor=lightblue,style=filled];
		11 [label="FilterExpression (11)",fillcolor=salmon,style=filled];
		5 [label="ProjectExpression (5)",fillcolor=lightblue,style=filled];
	}
	subgraph cluster_4{
		label="4";
		{rank=same; 4 18}
		4 [label="AggregationExpression (4)",fillcolor=lightblue,style=filled];
		18 [label="AggregationExpression (18)",fillcolor=salmon,style=filled];
	}
	subgraph cluster_6{
		label="6";
		{rank=same; 6 14}
		6 [label="FilterExpression (6)",fillcolor=lightblue,style=filled];
		14 [label="FilterExpression (14)",fillcolor=salmon,style=filled];
	}
	5 -> 6 [color=black];
	3 -> 5 [color=blue];
	1 -> 7 [color=red];
	2 -> 9 [color=red];
	3 -> 11 [color=red];
	6 -> 14 [color=red];
	1 -> 12 [color=red];
	5 -> 16 [color=red];
	4 -> 18 [color=red];
	18 -> 3 [color=black, lhead=cluster_3];
	14 -> 1 [color=black, lhead=cluster_1];
	16 -> 6 [color=black, lhead=cluster_6];
	11 -> 2 [color=black, lhead=cluster_2];
	9 -> 1 [color=black, lhead=cluster_1];
	6 -> 1 [color=black];
	4 -> 3 [color=black];
	3 -> 2 [color=black];
	2 -> 1 [color=black];
}
