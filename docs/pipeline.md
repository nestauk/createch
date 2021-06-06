# Pipeline to identify createch companies

## Gateway to Research

1. Fuzzy match gtr organisations with companies house
2. Predict disciplines for all GtR projects with long-ish descriptions
3. Tag projects that mention createch keywords
4. Find creative companies via SIC codes
5. Find projects involving these companies
6. Train a topic model on the corpus of creative projects
7. Find topics that are overrepresented in technology disciplines (cf #2) and identify createch within them
8. Find projects with high topic weights on createch topics _or_ presence of createch keywords
9. Identify organisations involved in those projects (includes creative and non-creative) that have been matched with companies house

## CrunchBase

1. Fuzzy match crunchbase with companies house
2. Create a labelled dataset of CB companies in creative SIC codes
3. Identify creative-related categories in CB
4. Build a corpus of creative SIC / creative category companies
5. Use SIC-labelled creative dataset to train a predictive model that we then apply to all companies in creative-related categories with long-ish descriptions (one goal is to filter companies where there is limited evidence of technological capabilities via the weight of the IT & computer programming label)
6. Tag projects that mention createch keywords
7. Train a topic model in the creative corpus
8. Identify createch related topics in the corpus
9. Find organisations with high createch topic weights or mentioning createch keywords
10. Select companies with predicted tech label above a threshold
11. Identify companies in this category that have been matched with companies house.
