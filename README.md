# Minimizing health costs of procedure delay during COVID-19

## Overview
During the COVID-19 pandemic, hospital systems postponed non-essential medical procedures to accomodate surge of critically-ill patients. We built a high-throughput pipeline that identifies procedures negatively impacted by COVID-19 and evaluates associations between procedure delay and patient outcomes. Our pipeline is implemented for EHRs using the OMOP Common Data Model.

**Reference**:

**Please direct questions to**: neil.zheng@vumc.org and wei-qi.wei@vumc.org

## Requirements
* OMOP Common Data Model Version 5
* R Packages:
  * dplyr
  * SqlRender
  * DatabaseConnector


## How to use
 * Clone or download repository in desired location. 
 * Update procedure_delay.r for your analysis:
   * Input database connection settings
   * Update procedure and diagnosis filters
   * Adjust SQL code as needed if using different table or view names 
 





