# Evaluating potential adverse patient outcomes associated with delay of procedures due to COVID-19

## Overview
During the coronavirus disease of 2019 (COVID-19), hospital systems postponed non-essential medical procedures to accommodate a surge of critically ill patients, and patients avoided hospital visits for routine healthcare. We developed two complementary approaches, one for inpatient surgical procedures (*surgical_delay.r*) and the other for outpatient screening tests (*screening_delay.r*), to help hospital systems understand and predict the impact of delaying procedures on patient health outcomes using historical electronic health record (EHR) data. 

**Reference**:

Please direct questions to neil.zheng@vumc.org and wei-qi.wei@vumc.org

## Requirements
* OMOP Common Data Model Version 5
* R Packages:
  * dplyr
  * SqlRender
  * DatabaseConnector
  * survival
  * lubridate
  * MASS


## How to use
 * Clone or download repository in desired location.
 * Update surgical_delay.r or screening_delay.r for your analysis:
   * Input database connection settings
   * Update procedure and diagnosis filters
   * Adjust SQL code as needed if using different table names
   * Adjust R code as needed for analysis if using different covariates or outcomes <br/>
   (Note: we included insurance information as a covariate and cancer stage at diagnosis as an outcome, both of which are not OMOP CDM)
 * Additional details included in surgical_delay.r or screening_delay.r 
 





