digraph G {


subgraph cluster_1 {
   out;
}
out -> filter1;

subgraph cluster_2 {
   filter1;
   project2;
   merge1;
   merge2;
}
filter1->project1;
project2->filter2;
merge1->filterP;
merge2->projectP;

subgraph cluster_3 {
   project1;
}
project1->table1;

subgraph cluster_4 {
   table1;
}


subgraph cluster_5 {
   filter2;
}

filter2->table1;


subgraph cluster_6 {
   filterP;
}

filterP->part1;

subgraph cluster_7 {
   part1;
}

part1->project1;

subgraph cluster_8 {
   projectP;
}

projectP->part2;

subgraph cluster_9 {
   part2;
}

part2->filter2;



}
