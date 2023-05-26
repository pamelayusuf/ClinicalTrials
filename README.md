# ClinicalTrials
This project contains the exploratory data analysis of clinical trials for the year 2021.

The datasets used for the analysis contains descriptions as shown below.

1. clinicaltrial2021.csv: each row represents an individual clinical trial, identifed by an Id,
listing the sponsor (Sponsor), the status of the study at time of the file's download (Status), the
start and completion dates (Start and Completion respectively), the type of study (Type), when the
trial was first submitted (Submission), and the lists of conditions the trial concerns (Conditions) and
the interventions explored (Interventions). Individual conditions and interventions are separated by
commas. (Source: ClinicalTrials.gov)

2. mesh.csv: the conditions from the clinical trial list may also appear in a number of hierarchies. The
hierarchy identifiers have the format [A-Z][0-9]+(_[A-Z][0-9]+)* (such as, e.g., D03.633.100.221.173)
where the initial letter and number combination designates the root of this particular hierarchy
(in the example, this is D03) and each \." descends a level down the hierarchy. The rows of this
file contain condition (term), hierarchy identier (tree) pairs. (Source: U.S. National Library of
Medicine.)

3. pharma.csv: the file contains a small number of a publicly available list of pharmaceutical violations.
For the puposes of this work, we are interested in the second column, Parent Company, which con-
tains the name of the pharmaceutical company in question. (Source: https://violationtracker.
goodjobsfirst.org/industry/pharmaceutic

The analysis was carried out in two implementations utilizing pyspark and HiveQL. Both implementations were carried out using the databricks file system.
