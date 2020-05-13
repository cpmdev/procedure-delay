###################################################################################
##                                                                               ##
##   This R script excecutes the procedure delay pipeline described              ##
##   at ____________ and was developed for OMOP Common Data Model version 5      ##
##                                                                               ##
##   Please email any questions to neil.zheng@vumc.org and wei-qi.wei@vumc.orb   ##
##                                                                               ##
###################################################################################


####################################
#       Preliminary setup          #
####################################

# Set working directory
setwd('/home/proc_delay')


# Loading required libraries
library(dplyr)
library(SqlRender)
library(DatabaseConnector)


# Defining database connection variables
cdmSchema = "cdm_v5"
dbms = "netezza" #Should be "sql server", "oracle", "postgresql" or "redshift"
driver_path = '' # path to database driver

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
#       Filtering and grouping procedures          #
#####################################################

# Creating table of filtered procedures
executeSql(conn, 
           "drop table proc_filtered if exists;
            create table proc_filtered as 
            select * from concept
            where vocabulary_id = 'proc4'
            -- evaluation and management (99201 – 99499)
            and not regexp_like(concept_code, '(^[9][9][2-4][0-9][0-9]$)', 'i') 
            -- anesthesia  (01000 – 01999; 99100 – 99140)
            and not regexp_like(concept_code, '(^[0][1][0-9][0-9][0-9]$)|([9][9][1][0-4][0-9])', 'i') 
            -- Radiology Services (70010 – 79999)
            and not regexp_like(concept_code, '^[7][0-9][0-9][0-9][0-9]$', 'i') 
            -- Pathology and Laboratory Services (80047 – 89398)
            and not regexp_like(concept_code, '^[8][0-9][0-9][0-9][0-9]$', 'i') 
            -- Misc uninformative procedures
            and not regexp_like(concept_name, '(collection of venous blood)|intubation', 'i')
            --- GRABBING ICD procedures
            union
            (select * from concept
            where vocabulary_id in ('ICD10PCS')
            and not regexp_like(concept_name, 'monitoring device', 'i'))
            ")


# Creating table of grouped procedures
executeSql(conn, 
           "drop table proc_group if exists;
           create table proc_group as 
           ---- CPT
           (select distinct'CPT' as SAB, DESCENDANT_CONCEPT_ID,  concept_name_d,  concept_code_d,
             ANCESTOR_CONCEPT_ID, c.CONCEPT_NAME, c.CONCEPT_CODE
             from
             -- grouping by minimum level of separation = 1
             (
               select distinct ANCESTOR_CONCEPT_ID, DESCENDANT_CONCEPT_ID, CONCEPT_NAME as concept_name_d, CONCEPT_CODE as concept_code_d
               from proc_filtered
               left join CONCEPT_ANCESTOR
               on concept_id=DESCENDANT_CONCEPT_ID
               where vocabulary_id='CPT4'
               and ANCESTOR_CONCEPT_ID!=DESCENDANT_CONCEPT_ID
               and min_levels_of_separation = 1
             ) as a 
             -- grabbing ancestor info
             inner join concept as c
             on c.concept_id = a.ancestor_concept_id
             -- joining with filtered table
             inner join proc_filtered as b
             on b.concept_id= a.DESCENDANT_CONCEPT_ID
             where c.vocabulary_id='CPT4'
           )
           ---- ICD10PCS
           union
           (select distinct 'ICD10PCS' as SAB, DESCENDANT_CONCEPT_ID,  concept_name_d,  concept_code_d,
             ANCESTOR_CONCEPT_ID, c.CONCEPT_NAME, c.CONCEPT_CODE
             from
             -- grouping by minimum level of separation = 2
             (
               select distinct ANCESTOR_CONCEPT_ID, DESCENDANT_CONCEPT_ID, CONCEPT_NAME as concept_name_d, CONCEPT_CODE as concept_code_d
               from proc_filtered
               left join CONCEPT_ANCESTOR
               on concept_id=DESCENDANT_CONCEPT_ID
               where vocabulary_id='ICD10PCS'
               and ANCESTOR_CONCEPT_ID!=DESCENDANT_CONCEPT_ID
               and min_levels_of_separation = 2
             ) as a 
             -- grabbing ancestor info
             inner join concept as c
             on c.concept_id = a.ancestor_concept_id
             -- joining with filtered table
             inner join proc_filtered as b
             on b.concept_id= a.DESCENDANT_CONCEPT_ID
             where c.vocabulary_id='ICD10PCS'
           )")


#########################################################
#       Counting procedures within timeframes           #
#########################################################
# Adjust below timeframes as needed
timeframe_2018 <- c('2018-03-19', '2018-04-25')
timeframe_2019 <- c('2019-03-18', '2019-04-24')
timeframe_2020 <- c('2020-03-18', '2020-04-24')


# Counting procedures in 2018
executeSql(conn,
           render(
             "drop table proc_count_2018 if exists;
              create table proc_count_2018 as 
              select ancestor_concept_id as concept_id, sab, concept_name, concept_code,
              -- counting total number of unique patients during window
              count(distinct proc.person_id) as count_2018, 
              (select count(distinct person_id) from v_visit_occurrence
               where visit_start_date between date('@a') and date('@b')
               and visit_concept_id in ('9201')) as total_2018
              from v_procedure_occurrence as proc
              -- joining with visit to get inpatient 
              inner join
              (select visit_occurrence_id from v_visit_occurrence as v
                where visit_concept_id in ('9201')) as visit
              on proc.visit_occurrence_id = visit.visit_occurrence_id
              -- getting procedure groupings
              inner join
              (select * from proc_group) as con
              on proc.procedure_source_value = con.concept_code_d
              -- selecting timeframe and grouping for counts
              where procedure_date between date('@a') and date('@b')
              group by concept_id, sab, concept_name, concept_code
              ",
             a = timeframe_2018[1],
             b = timeframe_2018[2]
           ))


# Counting procedures in 2019
executeSql(conn,
           render(
             "drop table proc_count_2019 if exists;
              create table proc_count_2019 as 
              select ancestor_concept_id as concept_id, sab, concept_name, concept_code,
              -- counting total number of unique patients during window
              count(distinct proc.person_id) as count_2019, 
              (select count(distinct person_id) from v_visit_occurrence
               where visit_start_date between date('@a') and date('@b')
               and visit_concept_id in ('9201')) as total_2019
              from v_procedure_occurrence as proc
              -- joining with visit to get inpatient 
              inner join
              (select visit_occurrence_id from v_visit_occurrence as v
                where visit_concept_id in ('9201')) as visit
              on proc.visit_occurrence_id = visit.visit_occurrence_id
              -- getting procedure groupings
              inner join
              (select * from proc_group) as con
              on proc.procedure_source_value = con.concept_code_d
              -- selecting timeframe and grouping for counts
              where procedure_date between date('@a') and date('@b')
              group by concept_id, sab, concept_name, concept_code
              ",
             a = timeframe_2019[1],
             b = timeframe_2019[2]
           ))


# Counting procedures in 2020
executeSql(conn,
           render(
             "drop table proc_count_2020 if exists;
              create table proc_count_2020 as 
              select ancestor_concept_id as concept_id, sab, concept_name, concept_code,
              -- counting total number of unique patients during window
              count(distinct proc.person_id) as count_2020, 
              (select count(distinct person_id) from v_visit_occurrence
               where visit_start_date between date('@a') and date('@b')
               and visit_concept_id in ('9201')) as total_2020
              from v_procedure_occurrence as proc
              -- joining with visit to get inpatient 
              inner join
              (select visit_occurrence_id from v_visit_occurrence as v
                where visit_concept_id in ('9201')) as visit
              on proc.visit_occurrence_id = visit.visit_occurrence_id
              -- getting procedure groupings
              inner join
              (select * from proc_group) as con
              on proc.procedure_source_value = con.concept_code_d
              -- selecting timeframe and grouping for counts
              where procedure_date between date('@a') and date('@b')
              group by concept_id, sab, concept_name, concept_code
              ",
             a = timeframe_2019[1],
             b = timeframe_2019[2]
           ))


# Exporting count data for analysis (left joining everything with anchor 2019 timeframe)
count <- querySql(conn,
                  "select proc_2019.*, COUNT_2020, TOTAL_2020, COUNT_2018, TOTAL_2018 from proc_count_2019 as proc_2019
                    -- joining with 2020 procedure counts
                    left join 
                    (select * from proc_count_2020) as proc_2020
                    on proc_2019.CONCEPT_CODE = proc_2020.CONCEPT_CODE 
                    -- joining with 2018 procedure counts
                    left join 
                    (select * from proc_count_2018) as proc_2018
                    on proc_2019.CONCEPT_CODE = proc_2018.CONCEPT_CODE
                   ")

# Writing table of negatively impacted procedures
write.table(count, './data/proc_counts.csv', sep = ',', row.names=FALSE)


############################################################
#       Identifying negatively impacted procedures         #
############################################################

# Counts for 2020 and 2018 timeframes were left joined to 2019 timeframe
# We need to replace NULL count values with 0 and NULL total values with the actual total
count$COUNT_2020[is.na(count$COUNT_2020)] <- 0
count$TOTAL_2020[is.na(count$TOTAL_2020)] <- count$TOTAL_2020[1]
count$COUNT_2018[is.na(count$COUNT_2018)] <- 0
count$TOTAL_2018[is.na(count$TOTAL_2018)] <- count$TOTAL_2018[1]


# Function to calculate relative ratio and 95% CI
calcRR <- function(count1, total1, count2, total2, ID){
  # Creating simpler variables
  A = count1
  B = total1 - count1
  C = count2
  D = total2 - count2
  
  # Calculating relative risk
  RR = (A/(A+B)) / (C/(C+D))
  
  # Calculating confidence interval (NOTE: does not account for relative risk of 0)
  CI_lower = exp(log(RR) - 1.96 * sqrt(1/A - 1/(A+C) + 1/B - 1/(B+D)))
  CI_upper = exp(log(RR) + 1.96 * sqrt(1/A - 1/(A+C) + 1/B - 1/(B+D)))
  
  # Return as dataframe
  return(data.frame(ID = ID, RR = RR, CI_lower = CI_lower, CI_upper = CI_upper))
}



# Using the relative ratio formula to determine procedures that were performed similarly frequently in 2018 and 2019
RR_2018 <- calcRR(count$COUNT_2018, count$TOTAL_2018, 
                  count$COUNT_2019, count$TOTAL_2019, 
                  ID = count$CONCEPT_CODE)
colnames(RR_2018) <- c('CONCEPT_CODE', 'RR_2018', 'CI_lower_2018', 'CI_upper_2018')


# Merging with counts 
merged <- merge(count, RR_2018, by='CONCEPT_CODE')


# Using 2019 proportion to determine procedures performed significantly fewer in 2020
merged$P_2020 <- pbinom(merged$COUNT_2020, merged$TOTAL_2019, 
                        merged$COUNT_2019/merged$TOTAL_2019) # One-tailed


# Filtering for significance, lower 95% CI 2018 <= 1, and filtering out certain procedure groups
filtered <- merged %>% 
  filter(P_2020 < 0.05 & CI_lower_2018 <= 1) %>% 
  filter(!grepl('(scop)|(imaging)|(diagnostic)|(measurement)|(administration)',CONCEPT_NAME,  ignore.case=TRUE)) %>%
  filter(!grepl('(extracorporeal)|(duplex scan)|(catheterization)|(echocardiography)',
                CONCEPT_NAME, ignore.case=TRUE)) %>%
  filter(!grepl('(vaginal delivery)|(obstetrics)|(anesthetic)|(auditory)',
                CONCEPT_NAME, ignore.case=TRUE)) %>%
  filter(!grepl('(infusion)|(PICC)|(Audio)|(ophthal)|(psycho)|(behavior)',
                CONCEPT_NAME, ignore.case=TRUE)) %>%
  filter(!grepl('(cognitive)',
                CONCEPT_NAME, ignore.case=TRUE)) %>%
  arrange(P_2020)


# Writing table of negatively impacted procedures
write.table(filtered, './output/proc_neg_impact_2020.csv', sep = ',', row.names=FALSE)


# Inserting negatively impacted procedures back into database
insertTable(conn, 
            tableName = "proc_filtered_binom", 
            data = filtered[,c('CONCEPT_ID', 'SAB', 'CONCEPT_NAME', 'CONCEPT_CODE')], 
            dropTableIfExists = TRUE, 
            createTable = TRUE, 
            tempTable = FALSE, 
            useMppBulkLoad = FALSE, 
            progressBar=TRUE)



############################################################
#           Mapping procedures to diagnoses                #
############################################################

# Creating table of procedures and condition co-occurrence
executeSql(conn,
           "drop table proc_cond_co_occurrence_binom if exists;
           create table proc_cond_co_occurrence_binom as
           select ancestor_concept_id as concept_id, sab, concept_code as proc_group, concept_name as proc_str, concept_id_icd, icd, icd_str, count(distinct proc.visit_occurrence_id) from procedure_occurrence as proc
           -- joining with visit to get only inpatient
           inner join
           (select visit_occurrence_id from visit_occurrence 
             where visit_concept_id = '9201') as visit
           on proc.visit_occurrence_id = visit.visit_occurrence_id
           -- getting procedure groupings
           inner join
           (select * from proc_group
             where ancestor_concept_id in
             (select concept_id from proc_filtered_binom)) as con
           on proc.procedure_source_value = con.concept_code_d
           -- joining all conditions
           inner join
           (select person_id, visit_occurrence_id, condition_start_date, 
             concept_id as concept_id_icd,
             condition_source_value as ICD, concept_name as icd_str
             from condition_occurrence as a
             inner join 
             (select * from concept
               where vocabulary_id in ('ICD10CM', 'ICD9CM')) as b
             on a.condition_source_concept_id  = b.concept_id)
           as cond
           on proc.visit_occurrence_id = cond.visit_occurrence_id 
           and proc.procedure_date = cond.condition_start_date
           group by  concept_id, sab, proc_group, proc_str, concept_id_icd, icd, icd_str
           "
)


# For our initial demonstration, we looked only at cancer and cardiac-related diagnoses
# We used phecode mappngs (icd10cm_mapping, icd9cm_mapping) to select relevant diagnoses. 
# Depending on the size of patient population in EHR system, this may be a neccessary filtering step to reduce
#   computational burden for the mapping steps that follow (as was the case in our implementation)
executeSql(conn,
           "drop table proc_cond_co_occurrence_filtered if exists;
           create table proc_cond_co_occurrence_filtered as 
           select * from proc_cond_co_occurrence_binom
           where icd in
           (select icd10 as ICD from icd10cm_mapping
            where regexp_like(phenotype, 'cancer|(^malignant neoplasm)', 'i')
            or regexp_like(phewas_code, '(^394)|(^4[0-5])', 'i')
            union 
            select icd9 as ICD from icd9cm_mapping
            where regexp_like(phenotype, 'cancer|(^malignant neoplasm)', 'i')
            or regexp_like(phewas_code, '(^394)|(^4[0-5])', 'i')
           )
           "
)


# Mapping procedure groups to descendent procedures and filtering out diagnoses-procedure pairs that co-occur < 50 times
executeSql(conn,
           "drop table procwas_desc if exists;
           create table procwas_desc as 
           select distinct a.*, descendant_concept_id as concept_id_d, concept_name_d, concept_code_d  
           from proc_cond_co_occurrence_filtered as a
           inner join
           (select * from proc_group) as b
           on a.concept_id = b.ancestor_concept_id
           where count >= 50
           "
)


# Identifying condition start date for each patient and diagnosis-procedure pair
# WARNING: This step creates a very big table and can take a long time.  
#          May require filterig in prior steps to reduce the number of diagnosis-procedure pairs.
executeSql(conn,
           "
           drop table procwas_cond if exists;
           create table procwas_cond as 
           select person_id, b.*, dx_date from 
           (select person_id, condition_source_concept_id as concept_id_icd, min(condition_start_date) as dx_date 
           from condition_occurrence
             where condition_source_concept_id in
             (select concept_id_icd from procwas_desc)
             group by person_id, concept_id_icd) as a
           inner join
           (select * from procwas_desc) as b
           on a.concept_id_icd = b.concept_id_icd
           "
)



# Identifying procedure start date for each patient and diagnosis-procedure pair
executeSql(conn,
           "drop table procwas_proc_cond if exists;
           create table procwas_proc_cond as 
           select cond.person_id, concept_id_icd, icd, icd_str, 
           concept_id as concept_id_proc, sab, proc_group, proc_str, 
           cond.concept_id_d, concept_name_d, concept_code_d,
           dx_date, min(proc_date) as proc_date 
           from procwas_cond as cond
           -- left joining with inpatient procedures
           left join
           (select person_id, procedure_concept_id as concept_id_d, procedure_date as proc_date
             from procedure_occurrence
             where visit_occurrence_id in 
             (select visit_occurrence_id from visit_occurrence 
              where visit_concept_id = '9201'))  as proc
           on cond.person_id = proc.person_id
           and cond.concept_id_d = proc.concept_id_d
           -- where diagnosis date is prior to procedure date
           and cond.dx_date <= proc.proc_date 
           group by cond.person_id, concept_id_icd, icd, icd_str, 
           concept_id_proc, sab, proc_group, proc_str, 
           cond.concept_id_d, concept_name_d, concept_code_d, dx_date
           "
)


# Regrouping procedures for the next step
executeSql(conn,
           "drop table procwas_proc_cond_group if exists;
           create table procwas_proc_cond_group as 
           (select person_id, concept_id_icd, icd, icd_str, 
           concept_id_proc, sab, proc_group, proc_str, 
           dx_date, 
           min(proc_date) as proc_date 
           from procwas_proc_cond
           group by person_id, concept_id_icd, icd, icd_str, concept_id_proc, sab, proc_group, proc_str, dx_date)
           "
)


# Finding diagnosis-procedure pairs where the proportion of diagnosed patients that receive procedure 
#   exceeds a given threshold (we chose >= 0.10).
prop_threshold = 0.10
executeSql(conn,
           render(
           "drop table procwas_proc_cond_filtered if exists;
           create table procwas_proc_cond_filtered as 
           (select concept_id_icd, icd, icd_str, 
             concept_id_proc, sab, proc_group, proc_str,
             sum(case when proc_date is null then 1 else 0 end) as null_count,
             sum(case when proc_date is null then 0 else 1 end) as case_count,
             case_count / (case_count + null_count) as case_prop
             from procwas_proc_cond_group 
             group by concept_id_icd, icd, icd_str, concept_id_proc, sab, proc_group, proc_str
             -- filtering pairs by proportion of diagnosed patients receiving procedure
             having case_prop >= @a
           )
           ",
           a = prop_threshold
           )
)






############################################################
#           Calculating procedure delay                    #
############################################################

# Filtering diagnosis-procedure pairs by the defined proportion threshold in previous step
# Calculates preliminary procedure delay
executeSql(conn,
           "drop table procwas_temp if exists;
           create table procwas_temp as
           select a.*, proc_date - dx_date as proc_delay from procwas_proc_cond as a
           inner join
           (select * from procwas_proc_cond_filtered) as b
           on a.concept_id_icd = b.concept_id_icd
           and a.concept_id_proc = b.concept_id_proc
           where proc_date is not null;
           "
)


# Regrouping procedures again and identifying first procedure date and minimum procedure delay per group
# Note: we regroup again here so we can keep the descendent procedure name for later review
executeSql(conn,
           "drop table procwas_delay if exists;
           create table procwas_delay as 
           select a.* from procwas_temp as a
           inner join
           (select person_id, concept_id_icd, icd, icd_str, 
             sab, concept_id_proc, proc_group, proc_str, dx_date, 
             min(proc_date) as proc_date, min(proc_delay) as delay
             from procwas_temp
             group by person_id, concept_id_icd, icd, icd_str, 
             sab, concept_id_proc, proc_group, proc_str, dx_date) as b
           on a.person_id = b.person_id
           and a.concept_id_icd = b.concept_id_icd
           and a.concept_id_proc = b.concept_id_proc
           and a.proc_date = b.proc_date
           "
)


# Filtering for patients with at least one visit prior to diagnosis
# Since VUMC is a tertiary care center, some patients may have diagnosis prior to arriving at VUMC
executeSql(conn,
           "drop table procwas_delay_final if exists;
           create table procwas_delay_final as 
           select p.* from procwas_delay as p
           -- Making sure each patient has at least 1 visit prior to their dx_date
           inner join
           (select a.person_id, a.icd, a.dx_date, count(distinct visit_occurrence_id) from procwas_delay as a
             inner join visit_occurrence as b
             on a.person_id = b.person_id 
             and visit_start_date < dx_date
             group by a.person_id, icd, dx_date
             having count > 0) as v
           on p.person_id = v.person_id
           and p.person_id = v.person_id
           and p.icd = v.icd
           and p.dx_date = v.dx_date
           "
)


# Pulling final procedure delay dataframe
# WARNING: This step may take a long time depending on how strict the prior filters were.
delay <- querySql(conn,
                  "select * from procwas_delay_final"
)

# Writing delays to csv
write.table(delay, './data/proc_delay.csv', sep=',', row.names=FALSE)


###################################################################
#           Creating patient outcome variables                    #
###################################################################

# Creating table for hospitalization length
executeSql(conn,
           "drop table procwas_hosp_length if exists;
           create table procwas_hosp_length as
           select distinct visit.person_id, visit.visit_occurrence_id, icd, icd_str, proc_group, proc_str, 
           proc_date, visit_end_date, visit_end_date - proc_date as hosp_length 
           from procwas_delay_final as a
           inner join
           (select * from procedure_occurrence
             where visit_occurrence_id in 
             (select visit_occurrence_id from visit_occurrence 
              where visit_concept_id = '9201')) as b
           on a.person_id = b.person_id 
           and a.proc_date = b.procedure_date
           and a.concept_id_d = b.procedure_concept_id
           -- inner joining visit table
           inner join
           visit_occurrence as visit
           on b.visit_occurrence_id = visit.visit_occurrence_id
           where visit_end_date is not null
           "
)


# Creating table for readmission within 30, 60, 90 days (inpatient or emergency)
executeSql(conn,
           " drop table procwas_readmin if exists;
           create table procwas_readmin as
           select distinct a.person_id, icd, icd_str, proc_group, proc_str, proc_date, b.visit_end_date,
           max(case when visit_start_date between b.visit_end_date + 1 and b.visit_end_date + 30 then 1 else 0 end) as readmin_30,
           max(case when visit_start_date between b.visit_end_date + 1 and b.visit_end_date + 90 then 1 else 0 end) as readmin_90,
           max(case when visit_start_date between b.visit_end_date + 1 and b.visit_end_date + 365 then 1 else 0 end) as readmin_365
           from visit_occurrence as a
           inner join
           (select * from procwas_hosp_length) as b
           on a.person_id = b.person_id
           -- Including inpatient or emergency visits
           where visit_concept_id in ('9201', '9203')
           and a.visit_occurrence_id not in (select visit_occurrence_id from procwas_hosp_length)
           group by a.person_id, icd, icd_str, proc_group, proc_str, proc_date, b.visit_end_date
           "
)


# Pulling outcomes data, including mortality within hospitalization 
outcomes <- querySql(conn,
                     "select distinct h.person_id, visit_occurrence_id, h.icd, h.proc_group, hosp_length, 
                     case when death_date between h.proc_date and h.visit_end_date then 1 else 0 end as hosp_death,
                     readmin_30, readmin_90, readmin_365
                     from procwas_hosp_length as h
                     -- joining with readmission data
                     full join
                     (select * from procwas_readmin) as r
                     on h.person_id = r.person_id 
                     and h.icd = r.icd
                     and h.proc_group = r.proc_group
                     and h.proc_date = r.proc_date
                     -- joining with death data
                     left join
                     (select * from death) as d
                     on h.person_id = d.person_id
                     "
                     )

# Writing outcomes to csv
write.table(outcomes, './data/proc_outcomes.csv', sep=',', row.names=FALSE)



###################################################################
#           Grabbing demographic data                             #
###################################################################

demo <- querySql(conn,
                 "select person_source_value as grid, a.person_id, gender_source_value as sex, race_source_value as race, 
                 year_of_birth
                 from person as a
                 inner join 
                 (select * from procwas_delay) as b
                 on a.person_id = b.person_id"
)

# Writing outcomes to csv
write.table(demo, './data/proc_demo.csv', sep=',', row.names=FALSE)



########################################################################################
#           Data cleaning and preparation for association analysis                     #
########################################################################################

# Loading procedure data 
delay <- read.table('./data/proc_delay.csv', sep = ',', header=TRUE, as.is=TRUE)
demo <- read.table('./data/proc_demo.csv', sep = ',', header=TRUE, as.is=TRUE)
outcomes <- read.table('./data/proc_outcomes.csv', sep = ',', header=TRUE, as.is=TRUE)

# General Equivalence Mapping (GEM) for ICD-9-CM to ICD-10-CM
gems <- read.table('./data/gems_cleaned.csv', sep = ',', header=TRUE, as.is=TRUE)

# ICD-10-CM descriptions
icd <- read.table('./data/icd10cm_desc.csv', sep = ',', header=TRUE, as.is=TRUE, quote="\"")


# Merging delay with demographic data and outcomes
proc <- merge(delay[,c('PERSON_ID', 'ICD', 'ICD_STR', 
                       'SAB', 'PROC_GROUP', 'PROC_STR', 
                       'DX_DATE', 'PROC_DATE', 'PROC_DELAY')] %>% distinct(),
              outcomes, by=c('PERSON_ID', 'ICD', 'PROC_GROUP'))
proc <- merge(demo, proc, by='PERSON_ID') %>% distinct()



# Fixing readmission numbers (no readmission found coded as NA, should be convered to 0)
proc$READMIN_30[is.na(proc$READMIN_30)] <-  0
proc$READMIN_90[is.na(proc$READMIN_90)] <-  0
proc$READMIN_365[is.na(proc$READMIN_365)] <-  0


# Converting ICD9CM to ICD10CM
proc_gems <- merge(proc, gems[gems$FLAG==0,], by.x='ICD', by.y='ICD9CM', all.x=TRUE)
proc_gems$ICD[!is.na(proc_gems$ICD10CM)] <- proc_gems$ICD10CM[!is.na(proc_gems$ICD10CM)]
proc_gems <- proc_gems[,-which(colnames(proc_gems) %in% c('ICD10CM'))]



# Recalculating delay based after ICD conversion (taking minimum diagnosis date of equivalent ICD-9-CM and ICD-10-CM)
proc_gems$DX_DATE <-  as.Date(proc_gems$DX_DATE )
proc_gems$PROC_DATE <-  as.Date(proc_gems$PROC_DATE )
proc_gems <- proc_gems %>% 
  group_by(PERSON_ID, GRID, SEX, RACE, YEAR_OF_BIRTH, 
           ICD, PROC_GROUP, PROC_STR, 
           PROC_DATE, 
           HOSP_LENGTH, HOSP_DEATH, READMIN_30, READMIN_90, READMIN_365) %>% 
  summarise(DX_DATE = min(DX_DATE), PROC_DELAY = max(PROC_DELAY))




# Getting ICD-10-CM description
proc_gems <- merge(proc_gems, icd, by.x='ICD', by.y='ICD10CM')
proc <- proc_gems[,c('PERSON_ID', 'GRID', 'SEX', 'RACE', 'YEAR_OF_BIRTH',
                     'ICD', 'ICD10CM_STR', 'PROC_GROUP', 'PROC_STR',
                     'DX_DATE', 'PROC_DATE', 'PROC_DELAY',
                     'HOSP_LENGTH', 'HOSP_DEATH', 'READMIN_30', 'READMIN_90', 'READMIN_365')]
proc <- proc %>% distinct()


# Adjusting procedure delay to weeks
proc$PROC_DELAY <- proc$PROC_DELAY / 7


# Creating age and procedure year variables
proc$DX_AGE =  as.numeric(format(as.Date(proc$DX_DATE),'%Y')) -  proc$YEAR_OF_BIRTH
proc$PROC_YEAR =  as.numeric(format(as.Date(proc$PROC_DATE),'%Y')) 


# Cleaning race variable
proc$RACE[grep('B', proc$RACE)] <- 'B'
proc$RACE[!grepl('B|^W$', proc$RACE)] <- 'O'


# Cleaning hosptilization length
proc$HOSP_LENGTH[proc$HOSP_LENGTH < 0] <- NA


########################################################################################
#           Association analysis of procedure delay and patient outcomes               #
########################################################################################


# Function to create results dataframe from regression analysis
analyze_proc_delay <- function(data, icd, procedure){
  # Creating empty dataframe for output
  out <-   data.frame(icd = character(), 
                      cpt = character(),
                      n = integer(),
                      outcome = character(),
                      beta_OR=double(),
                      CI_lower = double(),
                      CI_upper = double(),
                      p=double(),
                      stringsAsFactors=FALSE)
  
  # Subsetting data by ICD and CPT
  data_subset <- data[data$ICD == icd & data$PROC_GROUP == procedure,]
  
  
  # Creating formula based on if predictors have at least 2 factor levels
  f_str <- " ~ PROC_DELAY + DX_AGE + PROC_YEAR"
  if (length(unique(na.omit(data_subset)$SEX)) > 1){
    f_str <- paste(f_str, "+ SEX")
  }
  if (length(unique(na.omit(data_subset)$RACE)) > 1){
    f_str <- paste(f_str, "+ RACE")
  }
  
  
  # Hospitalization length 
  mod <- glm(as.formula(paste('HOSP_LENGTH', f_str)), data = data_subset)
  out[nrow(out) + 1, ] <- c(icd, procedure,nrow(data_subset),
                            'HOSP_LENGTH', 
                            coef(mod)[2], confint.default(mod)[2,1], confint.default(mod)[2,2], 
                            coefficients(summary(mod))[2,4])
  
  # Hospitalization death 
  if (length(unique(data_subset$HOSP_DEATH)) > 1){
    mod <- glm(as.formula(paste('HOSP_DEATH', f_str)), data = data_subset, family = 'binomial')
    out[nrow(out) + 1, ] <- c(icd, procedure, nrow(data_subset),
                              'HOSP_DEATH', 
                              exp(coef(mod)[2]), 
                              exp(confint.default(mod)[2,1]), 
                              exp(confint.default(mod)[2,2]),
                              coefficients(summary(mod))[2,4])
  }
  
  # Readmission rate
  mod <- glm(as.formula(paste('READMIN_30', f_str)), data = data_subset, family = 'binomial')
  out[nrow(out) + 1, ] <- c(icd, procedure, nrow(data_subset), 
                            'READMIN_30', 
                            exp(coef(mod)[2]), 
                            exp(confint.default(mod)[2,1]), 
                            exp(confint.default(mod)[2,2]),
                            coefficients(summary(mod))[2,4])
  
  mod <- glm(as.formula(paste('READMIN_90', f_str)), data = data_subset, family = 'binomial')
  out[nrow(out) + 1, ] <- c(icd, procedure, nrow(data_subset), 
                            'READMIN_90', 
                            exp(coef(mod)[2]), 
                            exp(confint.default(mod)[2,1]), 
                            exp(confint.default(mod)[2,2]),
                            coefficients(summary(mod))[2,4])
  
  mod <- glm(as.formula(paste('READMIN_365', f_str)), data = data_subset, family = 'binomial')
  out[nrow(out) + 1, ] <- c(icd, procedure, nrow(data_subset), 
                            'READMIN_365', 
                            exp(coef(mod)[2]), 
                            exp(confint.default(mod)[2,1]), 
                            exp(confint.default(mod)[2,2]),
                            coefficients(summary(mod))[2,4])
  
  # Returning final dataframe
  return(out)
}


# Finding unique ICD-PROCEDURE pairs
pairs <- proc %>% distinct(ICD, PROC_GROUP)


# Creating empty dataframe 
results <-   data.frame(icd = character(), 
                        cpt = character(),
                        outcome = character(),
                        beta_OR=double(),
                        CI_lower = double(),
                        CI_upper = double(),
                        p=double(),
                        stringsAsFactors=FALSE)


# Running regression analysis on each pair with function
for(i in 1:nrow(pairs)){
  if(i %% 100 == 0){
    print(sprintf('Completed %d of %d', i, nrow(pairs)))
  }
  results <- rbind(results, analyze_proc_delay(proc, pairs[i,1], pairs[i,2]))
}




# Getting ICD and Procedure names
results <- merge(proc[,c('ICD', 'ICD10CM_STR', 'PROC_GROUP', 'PROC_STR')] %>% distinct(), results,
                 by.x = c('ICD', 'PROC_GROUP'), by.y = c('icd', 'cpt'))

# Renaming and rearranging columns
colnames(results) <- c('ICD10CM', 'PROC_GROUP', 'ICD10CM_STR', 'PROC_STR', 
                       'N', 'OUTCOME', 
                       'BETA_OR', 'CI_LOWER', 'CI_UPPER', 'P')
results <- results[, c('ICD10CM', 'ICD10CM_STR','PROC_GROUP', 'PROC_STR', 
                       'N', 'OUTCOME', 
                       'BETA_OR', 'CI_LOWER', 'CI_UPPER', 'P')]


# Preparing final file and filtering for significant results
final <- results %>% filter(P < 0.05) %>% arrange(P)
final$BETA_OR <- as.numeric(final$BETA_OR)
final$CI_LOWER <- as.numeric(final$CI_LOWER)
final$CI_UPPER <- as.numeric(final$CI_UPPER)


# Writing to file
write.table(final, './output/proc_delay_results.csv',sep=',',row.names=FALSE)








