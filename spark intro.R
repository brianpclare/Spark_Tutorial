# install.packages("sparklyr")

library(sparklyr)

spark_install(version = "2.2.1")

# This may take a minute

conf <-  c(0)

conf$`sparklyr.cores.local` <- 2
conf$`sparklyr.shell.driver-memory` <- "3G"
conf$spark.memory.fraction <- 0.8

sc <- spark_connect(master = "local", 
                    version = "2.2.1",
                    config = conf)
# Wait...

library(dplyr)
library(nycflights13)
library(ggplot2)

flights <- copy_to(sc, flights, "flights")
airlines <- copy_to(sc, airlines, "airlines")
src_tbls(sc)

c1 <- filter(flights, day == 17, month == 5, carrier %in% c('UA', 'WN', 'AA', 'DL'))
c2 <- select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 <- arrange(c2, year, month, day, carrier)
c4 <- mutate(c3, air_time_hours = air_time / 60)

c4

carrier_hours <- collect(c4)

spark_disconnect_all()
