
# Synthetic Medicare Claims Data - not included in Github repo, over file size limit

# Outpatient Claims - 154 MB file
# https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DESample01.html

# Variable Guide
# https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/SynPUF_Codebook.pdf
# page 7, page 
library(tidyverse)
library(sparklyr)

library(data.table)

suppressWarnings(
  outpatient <- fread("outpatient claims.csv")
)

suppressWarnings(
  outpatient <- fread("outpatient claims.csv", nrows = 20)
)

# install.packages("icd")
library(icd)
icd_explain("4019")

outpatient$ICD9_DGNS_CD_1 <- icd_explain(outpatient$ICD9_DGNS_CD_1, condense = FALSE)

## Let's have a goal

# For each doctor, find the most common ICD9 diagnosis and list how many times that doctor treated that
# primary diagnosis, but only if that happened at least 5 times



# Now let's work with Spark

conf <-  c(0)

conf$`sparklyr.cores.local` <- 2
conf$`sparklyr.shell.driver-memory` <- "3G"
conf$spark.memory.fraction <- 0.8

sc <- spark_connect(master = "local", 
                    version = "2.2.0",
                    config = conf)



claims_data <- spark_read_csv(sc, name = "claims_data", path = "outpatient claims.csv")

claims_by_doctor <- claims_data %>% group_by(PRVDR_NUM, ICD9_DGNS_CD_1) %>% 
  summarize(count = n()) %>% filter(count = max(count)) %>% filter(count >= 5)


claims_by_doctor

result <- collect(claims_by_doctor)





spark_disconnect_all()

