### Loading required packages.
library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(foreach)
library(doParallel)
library(data.table)
library(pipeR)

## Set the driver of the PostgreSQL, the driver file (postgresql-9.4.1207.jar) must be downloaded first.
pgsql <- JDBC("org.postgresql.Driver", "C:/Users/Xiao Wang/Desktop/Programs/postgresql-9.4.1207.jar", "`")

## Establish the connection with Database Prod!
base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                user="u9dhckqe2ga9v1",
                password="pa49dck9aopgfrahuuggva497mh")

dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-54-243-198-3.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                      user="u95mf00g8knim4",
                      password="p4jm2l990uj4hl92dh9abf6qmkr")

## Set working directory.
setwd("C:/Users/Xiao Wang/Desktop/Programs/Projects/Prod_Revised/")

## Load all the user-defined functions. 
  # The source file should be put in the working directory.
source("C:/Users/Xiao Wang/Desktop/Programs/Github/SourceCodeBackUp/function_source.R")

#--------------------------------------------------------------------------------------------------#

## Loading drilling info data set here.
ndakota <- partition_load('WILLISTON', part_num = 10)

#------------#
cat('Production data was loaded successfully...\n')
#------------#

## Change data struture into data.table
ndakota <- as.data.table(ndakota)
## Set keys for faster searching.
setkey(ndakota, entity_id, basin, first_prod_year)

ndakota[, comment := ""]
ndakota[, last_prod_date := as.Date(last_prod_date)]
ndakota[, prod_date:= as.Date(prod_date)]

## choose the max date of available data
cutoff_date <- as.Date(max(ndakota[,prod_date]))

## change the liq into daily level.
## This could be done directly in the database.
# ndakota[, liq := liq/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = 'days')]


## Load decline rate data for all basins here.
dcl_all <- dbGetQuery(base, "select * from dev.zxw_nd_adj_log_dcl")
# dcl_all <- fread('C:/Users/Xiao Wang/Desktop/Programs/Projects/Prod_CO_WY/dcl_all_simple.csv')
dcl_all <- as.data.table(dcl_all)
setkey(dcl_all, basin, first_prod_year)
## Find all the basin names for current state.
basin_name <- unique(ndakota[, basin])
## Subset the decline rate table for faster matching.
dcl <- dcl_all[basin %in% basin_name, ]

#------------#
cat('Decline rate data was loaded successfully...\n')
#------------#

### Find entity with zero production.
zero <- sqldf("select entity_id
              from ndakota
              where last_prod_date = prod_date and liq = 0")

zero <- as.data.table(zero)
setkey(zero, entity_id)

### Load basin maximum production month table.
basin_max_mth_tbl <- dbGetQuery(base, "select * from dev.zxw_basin_max_mth")
basin_max_mth_tbl <- as.data.table(basin_max_mth_tbl)
setkey(basin_max_mth_tbl, basin, first_prod_year)

#-----------------------------------------------------------------#
# Part 1 -- Filling entity production which is actually not zero. #
#-----------------------------------------------------------------#
## In this part, two functions need to be loaded from function_source:
## @7th: filling_zero(), and @8th update_table()

### Main part.
numberOfWorkers = 3
cl_zero <- makeCluster(numberOfWorkers)
registerDoParallel(cl_zero)

#------------#
cat(sprintf('The number of threads is %i...\n', numberOfWorkers))
cat('#-------------------------------------------#\n')
cat(sprintf('Start filling zero at %s...\n', as.character(Sys.time())))
#------------#

tic_zero = proc.time() # Record the start time...
# collect all the updated entries.
fill_zero = foreach(i = 1:nrow(zero), .combine = rbind, .packages =  'data.table') %dopar%
  filling_zero(i, prod_tbl = ndakota)

# update the orignal table with values calculated in the last step.
ndakota <- update_table(orig_tbl = ndakota, update_val_tbl = fill_zero)
toc_zero = proc.time() # Record ending time.
time_usage_zero = toc_zero - tic_zero
time_usage_zero

stopCluster(cl_zero)
# replicate the filled table as a backup
ndakota_zero = ndakota
setkey(ndakota, entity_id, basin, first_prod_year)

#------------#
cat(sprintf('Filling zero was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')
#------------#


#-----------------------------------------#
# Part Two -- Filling the missing values. #
#-----------------------------------------#
cl_miss <- makeCluster(numberOfWorkers)
registerDoParallel(cl_miss)

#------------#
cat(sprintf('Start filling missing values at %s...\n',as.character(Sys.time())))
#------------#


## Entity with missing data
missing <- sqldf("with t0 as (
                 select entity_id, avg(liq) as avg
                 from ndakota
                 group by entity_id)
                 
                 select *
                 from ndakota
                 where entity_id in (select entity_id from t0 where avg >= round(20/30,4)) 
                 and prod_date = last_prod_date")

missing <- subset(missing, last_prod_date < cutoff_date)
missing <- as.data.table(missing)
setkey(missing, entity_id, basin, first_prod_year)
missing[, last_prod_date := as.character(last_prod_date)]
missing[, prod_date := as.character(prod_date)]

## Change data type for table union.
ndakota[, last_prod_date := as.character(last_prod_date)]
ndakota[, prod_date := as.character(prod_date)]

## Main program.
tic_missing = proc.time()
fill_miss = foreach(i = 1:nrow(missing), .combine = rbind, .packages = 'data.table') %dopar%
  filling_missing(i)

setkey(fill_miss, entity_id, n_mth)
# Update last_prod_date for entities who are filled.
last_prod_date_tbl <- fill_miss[!duplicated(entity_id, fromLast = T), .(entity_id, last_prod_date)]
updated_date = last_prod_date_tbl[1, last_prod_date]
# Add fill_miss table into original production table.
ndakota <- rbindlist(list(ndakota, fill_miss))
# Due to the cutoff date is set, so the last_prod_dates for filled data are the same.
ndakota[entity_id %in% last_prod_date_tbl[,entity_id], last_prod_date := updated_date]

toc_missing = proc.time()
time_usage_missing = toc_missing - tic_missing
time_usage_missing

ndakota_missing = ndakota
stopCluster(cl_miss)

setkey(ndakota, entity_id, basin, first_prod_year)
#------------#
cat(sprintf('Filling missing values was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')
#------------#


#---------------------------------------------#
# Part Three -- 15 month forward projection   #
#---------------------------------------------#

## Parallel computing setting.
cl_forward <- makeCluster(numberOfWorkers) # create the clusters.
registerDoParallel(cl_forward) # register the cluster setting to use multicores.


#------------#
cat(sprintf('Start making forward projection at %s...\n',as.character(Sys.time())))
#------------#

## Main Routine.
tic_forward = proc.time()
for (i in 1:15) {
  forward <- sqldf("with t0 as (
                   select entity_id, max(n_mth) as max
                   from ndakota
                   where comment != 'All Zeros'
                   group by entity_id),
                   
                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from ndakota a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 and comment != 'All Zeros'
                   group by a.entity_id)
                   
                   select entity_id
                   from t1
                   where avg >= round(20/30,4)")
  
  forward = as.data.table(forward)
  ndakota_last = ndakota[last_prod_date == prod_date, ]
  forward_dt = ndakota_last[entity_id %in% forward[, entity_id], ]
  
  # entities whose liq would remain constant.
  forward_const <- sqldf("with t0 as (
                   select entity_id, max(n_mth) as max
                   from ndakota
                   where comment != 'All Zeros'
                   group by entity_id),
                   
                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from ndakota a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 and comment != 'All Zeros'
                   group by a.entity_id)
                   
                   select entity_id
                   from t1
                   where avg < round(20/30,4) and avg > 0")
  forward_const <- as.data.table(forward_const)
  const_forward_dt = ndakota_last[entity_id %in% forward_const[, entity_id], ]
  
  # Making forward projection parallelly.
  # Cluster need to be set before.
  forward_liq_proj = foreach(j = 1:nrow(forward_dt), .combine = c, .packages = 'data.table') %dopar%
    forward_liq_func(j)
  
  ### Except liq and last_prod_date, changing the other columns outside the long iterations.
  ## entity_id, basin, first_prod_year remain the same.
  ## Only update values for last_prod_date, n_mth, prod_date, comment
  temp_ndakota_forward = forward_dt
  temp_ndakota_forward[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_forward[, n_mth:= (n_mth + 1)]
  temp_ndakota_forward[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_forward[, comment:= "Inserted"]
  temp_ndakota_forward[, liq:= forward_liq_proj]
  

  ### data table to store all the entities with constant forward production.
  temp_ndakota_const = const_forward_dt
  const_liq = const_forward_dt[, liq] # use the last availale data as the production.
  
  temp_ndakota_const[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_const[, n_mth:= (n_mth + 1)]
  temp_ndakota_const[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_const[, comment:= "Inserted"]
  temp_ndakota_const[, liq:= const_liq]
  
  # Update the last_prod_date for all the entities.
  ndakota[entity_id %in% forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  ndakota[entity_id %in% const_forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  
  # Append the forward prediction in original data set.
  ndakota = rbindlist(list(ndakota, temp_ndakota_forward,temp_ndakota_const))
  setkey(ndakota, entity_id, basin, first_prod_year)
  cat(sprintf('Congratulations! Iteration %i runs successfully...\n', i))
  cat(sprintf('Interation finished at %s...\n', as.character(Sys.time())))
}

toc_forward <- proc.time()
time_usage_forward <- toc_forward - tic_forward
time_usage_forward

stopCluster(cl_forward)

cat(sprintf('Forward projection was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')

#-------------------------------------------------------------#
# Part Four -- New production prediction                      #
#-------------------------------------------------------------#

#########################################################################################################
## forward prediction for new production

# sales price
hist_price <- dbGetQuery(dev_base, "with t0 as (
                         select (substr(contract,4,2)::INT + 2000) as year,
                         case when substr(contract,3,1) = 'F' then 1
                         when substr(contract,3,1) = 'G' then 2
                         when substr(contract,3,1) = 'H' then 3
                         when substr(contract,3,1) = 'J' then 4
                         when substr(contract,3,1) = 'K' then 5
                         when substr(contract,3,1) = 'M' then 6
                         when substr(contract,3,1) = 'N' then 7
                         when substr(contract,3,1) = 'Q' then 8
                         when substr(contract,3,1) = 'U' then 9
                         when substr(contract,3,1) = 'V' then 10
                         when substr(contract,3,1) = 'X' then 11
                         when substr(contract,3,1) = 'Z' then 12 end as month, avg(month1) as avg
                         from nymex_nearby
                         where tradedate >= current_date - interval '4 months' and product_symbol = 'CL'
                         group by year, month
                         order by 1, 2),
                         
                         t1 as (
                         select extract('year' from sale_date) as year, extract('month' from sale_date) as month, avg(price) as avg
                         from di.pden_sale
                         where prod_type = 'OIL' and sale_date >= '2013-10-01'
                         group by 1, 2
                         order by 1, 2)
                         
                         select * from t1
                         union
                         select * from t0
                         order by 1, 2                ")

f_price <- dbGetQuery(base, "select (substr(contract,4,2)::INT + 2000) as year,
                      case when substr(contract,3,1) = 'F' then 1
                      when substr(contract,3,1) = 'G' then 2
                      when substr(contract,3,1) = 'H' then 3
                      when substr(contract,3,1) = 'J' then 4
                      when substr(contract,3,1) = 'K' then 5
                      when substr(contract,3,1) = 'M' then 6
                      when substr(contract,3,1) = 'N' then 7
                      when substr(contract,3,1) = 'Q' then 8
                      when substr(contract,3,1) = 'U' then 9
                      when substr(contract,3,1) = 'V' then 10
                      when substr(contract,3,1) = 'X' then 11
                      when substr(contract,3,1) = 'Z' then 12 end as month, *
                      from nymex_nearby
                      where tradedate = (select max(tradedate) from nymex_nearby where product_symbol = 'CL') and product_symbol = 'CL'")

future_price <- as.data.frame(matrix(nrow = 11, ncol = 3));
colnames(future_price)<-c('year', 'month', 'avg');


## transform future price table
for (i in 1:11){
  
  if(f_price$month + i <= 12){
    future_price$year[i] <- f_price$year
    future_price$month[i] <- f_price$month + i
    future_price$avg[i] <- f_price[, (6+i)]
  } else {
    future_price$year[i] <- f_price$year + 1
    future_price$month [i]<- f_price$month + i - 12
    future_price$avg[i] <- f_price[, (6+i)]
  }
}

price <- as.data.frame(rbind(hist_price, future_price))


# prod of new wells
new_prod <- dbGetQuery(dev_base, "select first_prod_date, extract('year' from first_prod_date) first_prod_year,
                       extract('month' from first_prod_date) first_prod_month,
                       round(sum(liq)/1000/(extract(days from (first_prod_date + interval '1 month' - first_prod_date))),0) as new_prod
                       from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                       where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 and state = 'ND'
                       and first_prod_date < date_trunc('month', current_date)::DATE - interval '5 month' and first_prod_date = prod_date
                       group by 1,2,3
                       order by 1,2,3")

new_prod$first_prod_date <- as.character(new_prod$first_prod_date)

## prod

hist_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                        from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                        where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0
                        and state = 'ND' and prod_date < date_trunc('month', current_date)::DATE - interval '5 month'
                        group by prod_date
                        order by 1")

# hist_prod$prod_date <- as.Date(hist_prod$prod_date)

## forward prod from hist wells
prod <-  plyr::ddply(ndakota, 'prod_date', summarise, prod = sum(liq)/1000)

updated_prod <- prod[as.Date(prod$prod_date) > max(as.Date(hist_prod$prod_date)), ]

first_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                         from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                         where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0
                         and state = 'ND' and prod_date >= date_trunc('month', current_date)::DATE - interval '5 month' and prod_date = first_prod_date
                         group by prod_date
                         order by 1")

new_first_prod <- data.frame('prod_date' = rep(0,15), 'prod' = rep(0,15));
## prod from new wells and 15 month forward

#----------------------------------------------------------------------------------------#
### Calculate the state average decline rate.
monthly_prod = plyr::ddply(ndakota, .(prod_date, basin), summarise, basin_prod = sum(liq)/1000) %>>% as.data.table()
# Using the last five months production to calulate their weights.
prod_subset = monthly_prod[(prod_date <= '2015-11-01' & prod_date >= '2015-06-01'), ]
prod_subset[, basin_prod := basin_prod/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = c("days"))]
# Calculate the state total production
state_total = plyr::ddply(prod_subset, .(prod_date), summarise, state_prod = sum(basin_prod))
weight = sqldf("select a.*, round(basin_prod/state_prod,6) weight from prod_subset a, state_total b
               where a.prod_date = b.prod_date")
# Using the last five months' weights to calculate the average weight.
avg_weight = plyr::ddply(weight, .(basin), summarise, avg_weight = mean(weight)) %>>%
  as.data.table()

# using the average decline rate of last four years' (2012- 2015) 
# to make forward projection for new production . 
dcl_year_avg <- dcl[first_prod_year %in% c(2012:2015) & n_mth <= 20, .(avg = mean(avg)), by = .(basin, n_mth)]

dcl_weight_avg = dcl_year_avg
basin_name_ = avg_weight$basin

for(i in 1:length(basin_name_)){
  dcl_weight_avg[basin == basin_name_[i], .(avg = avg*avg_weight[basin == basin_name_[i], avg_weight])]
}

# calculate the weighted average decline rate for each first_prod_year and n_mth combination.
dcl_state_avg <- plyr::ddply(dcl_weight_avg, .(n_mth), summarise, avg = sum(avg))
dcl_state_avg <- as.data.table(dcl_state_avg)
setkey(dcl_state_avg, n_mth)
#----------------------------------------------------------------------------------------#


for (i in 1:20) {
  if(as.Date(format(as.Date(max(hist_prod$prod_date))+32,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
  {
    break
  }
  if(as.Date(format(as.Date(max(hist_prod$prod_date))+32,'%Y-%m-01')) <= as.Date(max(prod$prod_date)))
  {
    n = nrow(hist_prod)
    
    fit <- as.data.frame(cbind(new_prod$new_prod[2:n], new_prod$new_prod[1:(n-1)], hist_prod$prod[1:(n-1)], Moving_Avg(price$avg, 3)[1:(n-1),]))
    
    colnames(fit) <- c("new_prod","new_prod_lag", "prod", "avg")
    
    lm <- lm(new_prod ~ -1 + new_prod_lag + prod + avg, data = fit)
    
    #summary(lm)
    
    data <- as.data.frame(cbind(new_prod$new_prod[n], hist_prod$prod[n],Moving_Avg(price$avg, 3)[n,]))
    
    colnames(data) <- c("new_prod_lag", "prod", "avg")
    
    #prod of new wells
    new_prod[n+1,1] <- as.character(format(as.Date(new_prod$first_prod_date[n])+32,'%Y-%m-01'))
    new_prod[n+1,2] <- new_prod$first_prod_year[n]
    new_prod[n+1,3] <- new_prod$first_prod_month[n] + 1
    new_prod[n+1,4] <- round(predict(lm, data),0)
    
    # new first month production
    new_first_prod[i,1] <- as.character(format(as.Date(hist_prod$prod_date[n])+32,'%Y-%m-01'))
    new_first_prod[i, 2] <- round(predict(lm, data),0)
    
    # update the updated production
    temp <- new_first_prod[i,]
  
    if(new_first_prod[i,1] <= cutoff_date){
      temp <- new_first_prod[i, ]
    } else{
      for (j in 1:20) {
        if(as.Date(format(as.Date(temp$prod_date[1])+32*j,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
        {
          break
        }
        if(as.Date(format(as.Date(temp$prod_date[1])+32*j,'%Y-%m-01'))<= as.Date(max(prod$prod_date)))
        {
          m = nrow(temp)
          temp[m+1, 1] <- as.character(format(as.Date(temp$prod_date[j])+32,'%Y-%m-01'))

          dcl_factor <- 10^(dcl_state_avg[n_mth == (j + 1), avg]/100)
          temp[m+1, 2] <- round((1 + temp$prod[m]) * dcl_factor - 1,0)

          }
        }
      }

    
    # temp$prod_date <- as.Date(temp$prod_date)
    # updated_prod$prod_date <- as.Date(updated_prod$prod_date)
    updated_prod <- sqldf("select a.prod_date, a.prod + coalesce(b.prod, 0) as prod
                           from updated_prod a left join temp b
                           on a.prod_date = b.prod_date")
    
    ## new total production
    hist_prod[n+1,1] <- as.character(format(as.Date(hist_prod$prod_date[n])+32,'%Y-%m-01')) 
    
    temp_val <- if (length(first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]) == 0) {0
    } else {first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]}
    
    hist_prod[n+1,2] <- round(updated_prod$prod[updated_prod$prod_date == hist_prod$prod_date[(n+1)]] - temp_val, 0)
  }
}



EIA <- dbGetQuery(base, "select abbrev, date, value from ei.ei_flat
                  where abbrev = 'MCRFPND1' and value < 99999 and date >= '2015-01-01'")
EIA <- as.data.table(EIA)

EIA[, days := as.numeric(as.Date(format(as.Date(date) + 32, '%Y-%m-01')) - as.Date(date), unit = ('days'))]
