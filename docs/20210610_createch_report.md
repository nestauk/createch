---
title: Mapping the research and development landscape for creative technologies in the UK
author: Juan Mateos-Garcia
date: 10 June 2021
---
# 1. Executive summary
# 2. Introduction
Creative businesses are leading the development and deployment of emerging technologies such as Artificial Intelligence (AI), Augmented and Virtual Reality (AR / VR), simulations and blockchain. The use cases are wide-ranging: these technologies enable the creation of large and immersive environments populated by agents that behave in realistic ways with applications in sectors such as video-games, film production and architecture. Designers and advertisers use them to design and personalise products and adverts. Recent breakthroughs in AI-powered text and image generation have important use cases in media. Distributed ledger and blockchain systems can help creative firms keep track of and monetise their assets - the recent boom in NFTs ("Non-Fungible tokens") is an example of this. Increasing demand for culture, entertainment, communication and ecommerce during the pandemic have only bolstered demand for these technology-enhanced creative experiences and services.

All this illustrates the potential of creative technologies ("createch"), suggesting that they will play an increasing role driving innovation and growth in the creative industries in years to come. With its longstanding strengths in the arts, creativity, culture, computing and engineering, the UK is in an excellent position to realise these opportunities, strengthening its competitive position in creative markets and developing new, in some cases more inclusive, avenues for artistic and cultural expression.

Ensuring that these opportunities are realised will require supportive policies informed by robust evidence. The Createch programme of research led by the Creative Industries Council and the Policy and Evidence Centre for the Creative Industries in collaboration with TechNation and Nesta set out to contribute to this evidence base with analyses of the state and evolution of Createch and its drivers.

In recent reports, [TechNation have measured global investment trends in createch ventures](link), showing that the sector has experienced rapid growth and that the UK is a global hub for createch activity. [Their analysis of online job adverts](link) demonstrates that this growth in activity has been accompanied by increasing demand for createch occupations that combine creative and technology skills. Our work seeks to complement this analysis by focusing on the industrial and disciplinary composition and geography of createch R&D in the UK and its drivers through the lens of two datasets - [the Gateway to Research](link), which provides information about public (UK Research and Innovation) funded research in the UK, and [CrunchBase](link), a widely used startup database.

This research note presents emerging findings from our analysis of the Gateway to Research data addressing the following research questions:

* What are the levels of publicly funded R&D createch activity in the UK?
* What organisations and sectors are involved in it?
* How has this activity evolved over time?
* What is its geography?
* What is the disciplinary composition of createch research projects?
* What is the structure of research collaboration networks in various segments of createch activity?

Answering these questions will help us to understand the extent to which createch development is supported by the UK research system, and whether the increasing levels of private investment that is receiving from venture capitalists is reflected in public funding trends. We are also interested in measuring the levels of geographical concentrations of createch projects, and the extent to which createch projects combine different disciplines in ways that are more likely to generate innovative outcomes but could also create barriers to accessing funding.

In a follow-up report to be published in the Summer we will build on this analysis, combining it with our study of CrunchBase data, explore the drivers for the development of strong createch clusters and compare the outcomes of createch research projects and companies with a control group. The database of createch companies we have created in this project will also be used in a follow-up business survey by PEC researchers.

Next section summarises our data source and the methods we have used to identify Createch projects (see appendix for a detailed description). Having done this, we present our findings and discuss next steps.

# 3. Data sources and methods
## The Gateway to Research (GtR)
The Gateway to Research is an open database with information about projects funded by UKRI - including UK Research Councils and Innovate UK - going back (comprehensively) to 2007. It contains detailed metadata about projects such as their abstract, their research topics, starting and closing date and the organisations involved, the level of funding they received and project outputs including publications, patents, software,  datasets and cultural artifacts. We collect all this data from GtR's Application Programming Interface (API), yielding a corpus of 111,600 projects.

## Defining and identifying createch projects
For the purposes of this project, we define createch as technological research and development activities by creative firms. Identifying createch projects in the GtR projects requires taking two steps:

1. Identifying creative firms participating in research collaborations
2. Analysing the descriptions of these projects in order to determine if they are focused in developing new technologies

Step 1 requires determining the industrial sector of organisations in the Gatway to Research. To do this, we match all those organisations with Companies House using jacc-hammer, an internally developed fuzzy-matching algorithm that makes it possible to calculate similarities between the names of million of entities. We are able to find a high-confidence match in Companies House for 21,000 GtR organisations and analyse their Standard Industrial Classification (SIC) code in order to identify those that fulfil the [Department for Media, Culture and Sports (DCMS)](link) definition of the creative industries. This yields a final list of 3687 organisations (17% of the total) that have participated in 5,893 projects.^[This list includes a number of creative organisations such as museums, libraries and galleries, as well as the BBC, which are not present in Companies House. We identify them by looking for keywords in their Gateway to Research names and assigning them to a suitable sector]. 

Step 2 involves a semantic analysis of the descriptions of projects with participation from creative industries. We adopt two complementary strategies to achieve this:

* We train a topic model on project descriptions. This topic models returns 271 topics (clusters of terms that tend to happen in the same projects, suggesting that they belong to the same "theme") and a vector of weights indicating the probability that each of these topics is present in a creative industry projects. Having done this, we identify those topics that are related to technological disciplines.^[In order to do this we train a machine learning model that predicts the discipline for a project based on its text description. We build the labelled dataset to train this model using the research topic tags that researchers and funders assign to their projects. We will also use the resulting vector of probabilities to analyse the interdisciplinarity of research projects. We provide additional details about this procedure in the appendix.].
* We create a seed vocabulary of terms related to createch technologies and identify other terms that are semantically similar to them.^[We do this by training a word2vec model on the corpus of GtR project descriptions - this model represents all the terms ('words') in project descriptions in a vector space where those terms that are more semantically similar (i.e. more likely to appear in the same contexts) tend to be located closer to each other.] We then tag all creative projects that display those terms.

This process yields a list of 2,542 createch projects involving just over 4,000 organisations. Slightly more than half of these organisations (2,100) sit outside the creative industries.^[In other words, 2,100 organisations in non-creative SIC codes participate in projects that also involve at least one creative organisation - which critically, includes IT & Computer Programming firms. We note that some of these companies may in fact be creative firms misclassified in non-creative SICs. We explore the sectoral composition of non-creative participants in createch projects in section 4.b below).

# 4. Findings
## a. Examples
## b. Sectoral participation



## e. Interdisciplinarity
## f. Networks
# 5. Conclusions
## a. Implications
## b. Next steps
# Methodological annex
## Fuzzy matching
## Discipline labelling
## Topic modelling and semantic tagging


