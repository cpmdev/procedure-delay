###################################################################################
##                                                                               ##
##   This R script performs the screening procedure approach to assess           ##
##   the potential patient outcomes associated with screening or diagnostic      ##
##   test delay.                                                                 ##
##                                                                               ##
##   This script was developed for OMOP Common Data Model version 5              ##
##                                                                               ##
##   Please email any questions to neil.zheng@vumc.org and wei-qi.wei@vumc.org   ##
##                                                                               ##
###################################################################################


####################################
#       Preliminary setup          #
####################################

# Set working directory
setwd('WORKING_DIRECTORY')


# Loading required libraries
library(dplyr)
library(SqlRender)
library(DatabaseConnector)
library(survival)
library(lubridate)
library(MASS)

# Defining variables of interest
phecode = '153' # Colorectal cancer  153
procedure = 'colonoscopy' # Colonoscopy
guideline_age = 45 # Guideline age for colonoscopy


# Defining beginning of cease of elective procedures 
cease_date = '2020-03-18' # date format YYYY-MM-DD


####################################
#     Database connection          #
####################################


# Defining database connection variables
cdmSchema = "cdm_v5"
dbms = "netezza" #Should be "sql server", "oracle", "postgresql" or "redshift"
driver_path = 'PATH_TO_DRIVER' # Path to database driver

user <- "user"
pw <- "password"
server <- "server/database_name"
port <- "5480"


# Making connection 
connectionDetails <- createConnectionDetails(dbms=dbms, 
                                             server=server, 
                                             user=user, 
                                             password=pw, 
                                             port=port,
                                             pathToDriver = driver_path)
conn <- connect(connectionDetails)



#####################################################
#         Identifying patient cohort                #
#####################################################


# Loading ICD to phecode mappings
icd9cm_map <- read.table('./mapping/ICD9CM_to_Phecode_mapping.csv', sep=',', header=TRUE, as.is=TRUE, quote="\"")
icd10cm_map <- read.table('./mapping/ICD10CM_to_Phecode_mapping.csv', sep=',', header=TRUE, as.is=TRUE, quote="\"")


# Identifying ICDs of interest by phecode
icd_list <- icd9cm_map$ICD9[grepl(phecode, icd9cm_map$PHEWAS_CODE)]
icd_list <- c(icd_list, icd10cm_map$ICD10[grepl(phecode, icd10cm_map$PHEWAS_CODE)])


# Creating ICD string for SQL
icd_str <- paste(icd_list, collapse="\',\'")


# Identifying patients with diagnosis and grabbing earliest diagnosis date
executeSql(conn, 
           render(
             "drop table diag_person if exists;
             create table diag_person as 
             -- Identifying patients with at least 1 ICD code
             select distinct a.* from
             (select person_id, min(condition_start_date) as dx_date, count(distinct condition_occurrence_id)
             from condition_occurrence
             where condition_source_value in ('@icds')
             -- diagnosis prior to cease of elective procedures
             and condition_start_date < date('@date')
             group by person_id
             having count >= 2) as a
             -- Identifying patients with at least one outpatient visit prior to diagnosis
             inner join visit_occurrence as v
             on a.person_id = v.person_id
             and v.visit_start_date < a.dx_date
             where visit_concept_id = '9202'
              ",
             icds = icd_str,
             date = cease_date
           )
)



# Finding most recent procedure prior to diagnosis date - one month
executeSql(conn, 
           render(
             "
             drop table diag_proc if exists;
             create table diag_proc as 
             select distinct a.person_id, dx_date, procedure_date as proc_date
             from diag_person as a
             left join
             -- procedures 
             (select person_id, procedure_date from procedure_occurrence
               where procedure_concept_id in 
               (select concept_id from concept
                where regexp_like(concept_name, '@procedure', 'i')
                and domain_id = 'Procedure')
             -- some procedures are coded as measurements 
              union
              (select person_id, measurement_date as procedure_date from measurement
              where measurement_concept_id in
              (select concept_id from concept
              where regexp_like(concept_name, '@procedure', 'i')
              and domain_id = 'Measurement')
              )) as b
             on a.person_id = b.person_id
             and b.procedure_date < a.dx_date - 30
             ",
             procedure = procedure)
)




#########################################################
#      Identifying covariates and outcomes              #
#########################################################

###### WARNING: INSURANCE AND CANCER STAGE AT DIAGNOSIS ARE NOT OMOP, PLEASE ADJUST AS NEEDED ######


# Pulling final data, including demographic and survival information
data <- querySql(conn,
                 "-- demographic data
                  select distinct p.person_id, person_source_value as grid, 
                  gender_source_value as sex, race_source_value as race, birth_datetime as birth_date,
                  dx_date, proc_date, death_date, last_follow_date
                  from person as p
                  -- inner join with patient cohort and procedures
                  inner join diag_proc as dp
                  on p.person_id = dp.person_id
                  -- left join with death table
                  left join 
                  (select * from death
                    where death_date is not null)as d
                  on p.person_id = d.person_id
                  -- left join with visit to get date of last follow-up
                  left join
                  (select person_id, max(visit_start_date) as last_follow_date 
                  from visit_occurrence 
                  group by person_id) as v
                  on p.person_id = v.person_id
                  "
)

# Writing to final data csv
write.table(data, paste('./data/screen_diag_proc_',phecode,'.csv', sep=''), sep = ',', row.names=FALSE)


##### Pulling insurance data (ADJUST AS NEEDED SINCE THIS IS NOT OMOP) #####
insurance <- querySql(conn,
                      "select a.*, INSURANCE_TYPE_1 as INSURANCE, COUNT from diag_proc as a
                 left join
                 (select person_id, insurance_type_1, count(visit_occurrence_id) from enc_insurance
                  group by person_id, insurance_type_1) as b
                 on a.person_id = b.person_id
                  "
)

# Writing insurance data to csv
write.table(insurance, paste('./data/screen_insurance_',phecode,'.csv', sep=''), sep = ',', row.names=FALSE)


##### Pulling cancer stage (ADJUST AS NEEDED SINCE THIS IS NOT OMOP) #####
cancer_stage <- querySql(conn,
                         "
                         select distinct a.person_id,
                         case when tnm_path_stage_group in ('99', '88', '') then null
                         else tnm_path_stage_group end as path_stage, 
                         case when tnm_clin_stage_group in ('99', '88', '') then null
                         else tnm_clin_stage_group end as clin_stage, 
                         case when path_stage is not null then path_stage
                         else clin_stage end as dx_stage
                         from tr_naaccr as a
                         -- finding cancer registry entry closest to ICD diagnosis date
                         inner join
                         (select distinct cancer_registry.person_id, min(date_dx) as date_dx
                           from tr_naaccr as cancer_registry
                           inner join diag_proc as diag_proc
                           on cancer_registry.person_id = diag_proc.person_id
                           where abs(dx_date - date_dx) <= 90
                           group by cancer_registry.person_id) as b
                         on a.person_id = b.person_id
                         and a.date_dx = b.date_dx
                         "
)


# Writing cancer stage data to csv
write.table(cancer_stage, paste('./data/screen_cancer_stage_',phecode,'.csv', sep=''), sep = ',', row.names=FALSE)




#########################################################
#          Preparing variables for analysis             #
#########################################################

###### WARNING: INSURANCE AND CANCER STAGE AT DIAGNOSIS ARE NOT OMOP, PLEASE ADJUST AS NEEDED ######


#####  Loading data ##### 
data <- read.table(paste('./data/screen_diag_proc_',phecode,'.csv', sep=''), 
                   sep = ',', header=TRUE, as.is=TRUE)
insurance <- read.table(paste('./data/screen_insurance_',phecode,'.csv', sep=''), 
                        sep = ',', header=TRUE, as.is=TRUE)
cancer_stage <- read.table(paste('./data/screen_cancer_stage_', phecode,'.csv', sep=''), 
                           sep = ',', header=TRUE, as.is=TRUE)

#####  Cleaning race variable ##### 
data$RACE[grep('B', data$RACE)] <- 'B'
data$RACE[!grepl('B|^W$', data$RACE)] <- 'O'


##### Creating time related variables ##### 
# Converting dates from string to Date objects
data$BIRTH_DATE <- as.Date(data$BIRTH_DATE)
data$DX_DATE <- as.Date(data$DX_DATE)
data$PROC_DATE <- as.Date(data$PROC_DATE)
data$DEATH_DATE <-as.Date(data$DEATH_DATE)
data$LAST_FOLLOW_DATE <-as.Date(data$LAST_FOLLOW_DATE)

# Subsetting for patients over guideline age and creating variable for date when patient should start receiving 
#   procedure based on age guidelines
data$DX_AGE =  round((data$DX_DATE - data$BIRTH_DATE) / 365.25, 2)
data <- data[data$DX_AGE >= guideline_age, ]
data$GUIDELINE_DATE =  data$BIRTH_DATE + years(50)


# Finding most recent procedure and calculating procedure frequency
data <- data %>% group_by(PERSON_ID) %>% 
  # Filtering for procedures after guideline start
  filter(PROC_DATE >= GUIDELINE_DATE) %>%
  # Identifying first procedure, most recent procedure, and procedure count
  mutate(PROC_DATE = max(PROC_DATE), PROC_COUNT = length(PROC_DATE)) %>%
  # Calculating procedure frequency
  mutate(PROC_FREQ = (DX_DATE - GUIDELINE_DATE)/PROC_COUNT/365.25) %>%
  distinct() %>%
  arrange(PERSON_ID, PROC_DATE)



# Creating age and procedure year variables
data$PROC_YEAR =  as.numeric(format(as.Date(data$PROC_DATE),'%Y')) 

# Estimating procedure delay in 6 months (time from diagnosis to last diagnostic procedure)
data$PROC_DELAY <- (data$DX_DATE - data$PROC_DATE) / (365.25 / 2)

# Estimating 3 year, 5 year survival and creating outcomes
data$SURV_STATUS_3 <- ifelse(!is.na(data$DEATH_DATE) & (data$DEATH_DATE - data$DX_DATE)/365.25 <= 3,
                             TRUE, FALSE)
data$SURV_TIME_3 <- ifelse(data$SURV_STATUS_3, 
                           data$DEATH_DATE - data$DX_DATE,
                           pmin(data$LAST_FOLLOW_DATE - data$DX_DATE, 3 * 365.25)) 

data$SURV_STATUS_5 <- ifelse(!is.na(data$DEATH_DATE) & (data$DEATH_DATE - data$DX_DATE)/365.25 <= 5,
                             TRUE, FALSE)
data$SURV_TIME_5 <- ifelse(data$SURV_STATUS_5, 
                           data$DEATH_DATE - data$DX_DATE,
                           pmin(data$LAST_FOLLOW_DATE - data$DX_DATE, 5 * 365.25)) 




##### Creating insurance status #####  (ADJUST AS NEEDED SINCE THIS IS NOT OMOP)
# Grouping insurances
# Commercial: Blue Cross, Blue Shield, Commerecial, HMO, Exchange
insurance$INSURANCE[grepl('Blue|Commercial|HMO|Exchange', insurance$INSURANCE, 
                          perl=TRUE, ignore.case = TRUE)] <- 'Commercial'
# Government
insurance$INSURANCE[grepl('Champus|Government', insurance$INSURANCE, 
                          perl=TRUE, ignore.case = TRUE)] <- 'Government'
# Medicare
insurance$INSURANCE[grepl('Medicare', insurance$INSURANCE, 
                          perl=TRUE, ignore.case = TRUE)] <- 'Medicare'
# Medicaid/TennCare
insurance$INSURANCE[grepl('Medicaid|TennCare', insurance$INSURANCE, 
                          perl=TRUE, ignore.case = TRUE)] <- 'Medicaid'
# Other
insurance$INSURANCE[!(insurance$INSURANCE %in% c('Commercial', 'Government', 'Medicare', 'Medicaid'))] <- 'Other'


# Getting most common insurance type per person
insurance <- insurance %>% group_by(PERSON_ID) %>% slice(which.max(COUNT)) %>% distinct()

# Merging with main dataframe
data <- merge(data, insurance[,c('PERSON_ID', 'INSURANCE')], by = 'PERSON_ID') %>% distinct()


##### Creating cancer stage variables #####  (ADJUST AS NEEDED SINCE THIS IS NOT OMOP)
cancer_stage$DX_STAGE[grep('0|O', cancer_stage$DX_STAGE)]  <- 0
cancer_stage$DX_STAGE[grep('1', cancer_stage$DX_STAGE)]  <- 1
cancer_stage$DX_STAGE[grep('2', cancer_stage$DX_STAGE)]  <- 2 
cancer_stage$DX_STAGE[grep('3', cancer_stage$DX_STAGE)]  <- 3
cancer_stage$DX_STAGE[grep('4', cancer_stage$DX_STAGE)]  <- 4
cancer_stage$DX_STAGE <- as.factor(cancer_stage$DX_STAGE)

# Merging cancer data
data <- merge(data, cancer_stage[,c('PERSON_ID', 'DX_STAGE')], by='PERSON_ID', all.x=TRUE)


#########################################################
#          Cox Proportional Hazards Model               #
#########################################################

###### NOTE: PLEASE ADJUST COVARIATES AS NEEDED ######

# Creating empty dataframe to contain results
results <-   data.frame(PHECODE = character(), 
                        PROCEDURE = character(),
                        OUTCOME = character(),
                        N = integer(),
                        EVENTS = integer(),
                        BETA_OR=double(),
                        CI_LOWER = double(),
                        CI_UPPER = double(),
                        P=double(),
                        stringsAsFactors=FALSE)


# Creating formula string (ADJUST COVARIATES AS NEEDED)
form_str <- "PROC_DELAY + RACE + DX_AGE + PROC_FREQ + PROC_YEAR + INSURANCE"

# If SEX is not all M or F, add sex as covariate
if(length(unique(data$SEX)) > 1){
  from_str <- paste(form_str, '+ SEX')
}

# 3-year survival Cox proportional hazards
cox_3 <- coxph(as.formula(paste('Surv(SURV_TIME_3, SURV_STATUS_3) ~', form_str)), 
               data = data)
# Adding to results dataframe
results[nrow(results) + 1, ] <- c(phecode,
                                  procedure, 
                                  '3_YEAR_SURVIVE', 
                                  cox_3$n,
                                  cox_3$nevent,
                                  exp(coef(cox_3)[1]), 
                                  exp(confint.default(cox_3)[1,1]), 
                                  exp(confint.default(cox_3)[1,2]),
                                  coef(summary(cox_3))[1,5])


# 5-year survival Cox proportional hazards
cox_5 <- coxph(as.formula(paste('Surv(SURV_TIME_5, SURV_STATUS_5) ~', form_str)), 
               data = data)
# Adding to results dataframe
results[nrow(results) + 1, ] <- c(phecode,
                                  procedure, 
                                  '5_YEAR_SURVIVE', 
                                  cox_5$n,
                                  cox_5$nevent,
                                  exp(coef(cox_5)[1]), 
                                  exp(confint.default(cox_5)[1,1]), 
                                  exp(confint.default(cox_5)[1,2]),
                                  coef(summary(cox_5))[1,5])



#########################################################
#         Ordinal logistic regression                   #
#########################################################

###### NOTE: PLEASE ADJUST COVARIATES AS NEEDED ######

# Cancer stage ordinal logistic regression
ord_stage <- polr(as.formula(paste('DX_STAGE ~', form_str)), 
                  data = data)

# Calculating p-value
p <- 2*pt(abs(coef(summary(ord_stage))[1,3]), ord_stage$n, lower.tail=FALSE)

# Adding to results dataframe
results[nrow(results) + 1, ] <- c(phecode,
                                  procedure, 
                                  'CANCER_STAGE', 
                                  ord_stage$n,
                                  NA,
                                  exp(coef(ord_stage)[1]), 
                                  exp(confint.default(ord_stage)[1,1]), 
                                  exp(confint.default(ord_stage)[1,2]),
                                  p)


results$OR_FORMAT <- mapply(function(x,y,z) sprintf('%.2f (%.2f to %.2f)', x,y,z), 
                            as.numeric(results$BETA_OR), 
                            as.numeric(results$CI_LOWER), 
                            as.numeric(results$CI_UPPER))

# Writing out results
write.table(results, paste('./output/screen_delay_', phecode, '.csv', sep = ''), 
            sep = ',', row.names=FALSE)





