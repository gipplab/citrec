# Source code for the open evaluation framework CITREC#
**See http://www.sciplore.org/projects/citrec**  

The code includes:

1. parsers for the PMC OAS and the TREC Genomics collection as well as tools to retrieve [MeSH ](http://www.nlm.nih.gov/mesh/introduction.html) and article metadata from [NCBI](http://www.ncbi.nlm.nih.gov/) resources (package org.sciplore.citrec.dataimport)

2. tools to statistically evaluate retrieval results using a top-k or a rank-based analysis (package org.sciplore.citrec.eval)

3. implementations of similarity measures and code to calculate the MeSH-based gold standard (package org.sciplore.citrec.sim)