library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(kimisc)
library(doParallel)



pgsql <- JDBC("org.postgresql.Driver", "C:/postgresql-9.2-1003.jdbc4.jar", "`")

base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="u9dhckqe2ga9v1",password="pa49dck9aopgfrahuuggva497mh")

#dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-107-20-245-110.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="ubp42sjidmq53r",password="p4h3atl4j4ekqc9vmgtcrrku8as")

options("scipen"=100)
#############################################################################################################################################################


## entity with missing date
ndakota <- dbGetQuery(base, "select * from dev.zsz_nd_dec")

ndakota <- mutate(ndakota, comment = "")

ndakota$last_prod_date <- as.Date(ndakota$last_prod_date)

ndakota$prod_date <- as.Date(ndakota$prod_date)


## prod dcl fro williston basin

# Version 1.
dcl <- dbGetQuery(base, "select * from dev.zsz_williston_dcl_dec")
max_dcl_year <- max(dcl$first_prod_year)
max_dcl_mth <- max(dcl$n_mth[dcl$first_prod_year == max_dcl_year])

temp_dcl <- dcl[dcl$first_prod_year == (max_dcl_year - 1) & dcl$n_mth > max_dcl_mth, ]
temp_dcl$first_prod_year <- max_dcl_year

dcl <- rbind(dcl, temp_dcl)

# Version 2. -- Log version
dcl_all = fread("dcl_all.csv")
dcl = dcl_all

## check zeros

zero <- sqldf("select entity_id 
              from ndakota 
              where last_prod_date = prod_date and liq = 0")

# set the table class into data.table
ndakota = as.data.table(ndakota)
dcl = as.data.table(dcl)
zero = as.data.table(zero)
# head(ndakota)
# head(dcl)
# head(zero)

# set key for for the table to make faster searching and matching.
setkey(ndakota, entity_id, basin, first_prod_year)
setkey(dcl, basin, first_prod_year)
setkey(zero, entity_id)

tic_zero = Sys.time() # Record the program start time.

for (i in 1:nrow(zero)) {
  #choose prod data for past 6 month
  temp <- ndakota[entity_id == zero[i,entity_id],]
  temp_entity_id <- temp[1,entity_id]
  temp_basin <- temp[1,basin]
  
  if (temp[1,first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[1,first_prod_year]
    }
  
  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- max(dcl[basin == temp_basin & first_prod_year == first, n_mth])
  
  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]
  
  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
      dcl_mth <- basin_max_mth
  } else {
    dcl_mth <- max_n_mth
  }
  
  if (temp[n_mth == (max_n_mth - 1),liq] == 0) { 
    
    if (temp[n_mth == (max_n_mth - 2),liq] == 0) {
      
      if (temp[n_mth == (max_n_mth - 3),liq] == 0) {
        
        ndakota[entity_id == temp_entity_id, comment:= "All Zeros"]
      } else {
        
        ndakota[(n_mth == (max_n_mth - 2) & entity_id == temp_entity_id), liq:= temp[n_mth == (max_n_mth - 3), liq] * ( 1 + dcl[(first_prod_year == first & n_mth == (dcl_mth - 2) & basin == temp_basin), avg]/100)]
        ndakota[(n_mth == (max_n_mth - 2) & entity_id == temp_entity_id), comment:= "Updated"]
        
        ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id), liq:= temp[n_mth == (max_n_mth - 3), liq] * ( 1 + dcl[(first_prod_year == first & n_mth == (dcl_mth - 1) & basin == temp_basin), avg]/100)]
        ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id),comment:="Updated"]
        
        ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), liq:= ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id), liq]  * ( 1 + dcl[(first_prod_year == first & n_mth == dcl_mth & basin == temp_basin), avg]/100)]
        ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), comment:="Updated"]
      }
      
      
    } else {
      
      ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id), liq:=temp[(n_mth == (max_n_mth - 2)), liq] * ( 1 + dcl[(first_prod_year == first & n_mth == (dcl_mth - 1) & basin == temp_basin), avg]/100)]
      ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id), comment:="Updated"]
      
      
      ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), liq:= ndakota[(n_mth == (max_n_mth - 1) & entity_id == temp_entity_id), liq] * ( 1 + dcl[(first_prod_year == first & n_mth == dcl_mth & basin == temp_basin), avg]/100)]
      ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), comment:="Updated"]
    }
    
  } else {
    
    ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), liq:= temp[(n_mth == (max_n_mth - 1)), liq] * ( 1 + dcl[(first_prod_year == first & n_mth == dcl_mth & basin == temp_basin), avg]/100)]
    ndakota[(n_mth == max_n_mth & entity_id == temp_entity_id), comment:="Updated"]
  }
}

toc_zero = Sys.time() # Record the program end time.
time_usage_zero = toc_zero - tic_zero
time_usage_zero
ndakota_zero = ndakota

# ndakota = ndakota_zero

## entity with missing data
missing <- sqldf("with t0 as (
                 select entity_id, avg(liq) as avg
                 from ndakota
                 group by entity_id)
                 
                 select * 
                 from ndakota
                 where entity_id in (select entity_id from t0 where avg >= 20) and prod_date = last_prod_date")

missing <- subset(missing, last_prod_date < '2015-11-01')

## Filling missing data ################################################################################################################################

missing = as.data.table(missing)
# head(missing)
setkey(missing, entity_id, basin, first_prod_year)

# i = 1

# change the data type for table union.
ndakota[, last_prod_date:= as.character(last_prod_date)]
ndakota[, prod_date:= as.character(prod_date)]

tic_missing = Sys.time()

for (i in 1:nrow(missing)) {
  temp <- missing[i,]
  temp_entity_id <- temp[, entity_id]
  temp_basin <- temp[, basin]
  
  
  # if (temp$first_prod_year[1] < 1980) {
  if (temp[1, first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[1, first_prod_year]
    }
  
  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- max(dcl[basin == temp_basin & first_prod_year == first, n_mth])
  
  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]
  
  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth
    
    # j = 0
    for(j in 1:5)
    {
      if(as.Date(format(as.Date(temp[, prod_date]) + 32*j,'%Y-%m-01')) > as.Date('2015-11-01'))  
      {
        break
      }
      if(as.Date(format(as.Date(temp[, prod_date]) + 32*j,'%Y-%m-01')) <= as.Date('2015-11-01'))
      {
        n = nrow(ndakota)

        ndakota[(entity_id == temp_entity_id), last_prod_date:= as.character(format(as.Date(temp[,last_prod_date])+32*j,'%Y-%m-01'))]
        
        if(j == 1) {
          temp_liq = round(temp[,liq] * ( 1 + dcl[(first_prod_year == first & n_mth == dcl_mth & basin == temp[,basin]), avg]/100),0)
        } else {
          temp_liq = round(ndakota[n,liq] * ( 1 + dcl[(first_prod_year == first & n_mth == dcl_mth & basin == temp[,basin]), avg]/100),0)
        }
        

        temp_dt = data.table(
          "entity_id" = temp_entity_id,
          "basin" = temp_basin,
          "first_prod_year" = temp[,first_prod_year],
          "last_prod_date" = as.character(format(as.Date(temp[,last_prod_date])+32*j,'%Y-%m-01')),
          "n_mth" = (temp[,n_mth] + j),
          "prod_date" = as.character(format(as.Date(temp[,prod_date])+32*j,'%Y-%m-01')),
          "liq" = temp_liq,
          "comment" = "Inserted")
        ndakota = rbindlist(list(ndakota, temp_dt))
      }
    }
    
  } else {
    dcl_mth <- max_n_mth 
    
    for(j in 1:5)
    {
      if(as.Date(format(as.Date(temp[,prod_date])+32*j,'%Y-%m-01')) > as.Date('2015-11-01'))
      {
        break
      }
      if(as.Date(format(as.Date(temp[,prod_date])+32*j,'%Y-%m-01')) <= as.Date('2015-11-01'))
      {
        n = nrow(ndakota)
        ndakota[(entity_id == temp_entity_id), last_prod_date:= as.character(format(as.Date(temp[,last_prod_date])+32*j,'%Y-%m-01'))]
        
        if(j == 1) {
          temp_liq = round(temp[,liq] * ( 1 + dcl[(first_prod_year == first & n_mth == (dcl_mth + j) & basin == temp_basin), avg]/100),0)
        } else {
          temp_liq = round(ndakota[n, liq] * ( 1 + dcl[(first_prod_year == first & n_mth == (dcl_mth + j) & basin == temp_basin), avg]/100),0)
        }
        
        temp_dt = data.table(
          "entity_id" = temp_entity_id,
          "basin" = temp_basin,
          "first_prod_year" = temp[,first_prod_year],
          "last_prod_date" = as.character(format(as.Date(temp[,last_prod_date]) +32*j,'%Y-%m-01')),
          "n_mth" = (temp[,n_mth] + j),
          "prod_date" = as.character(format(as.Date(temp[,prod_date]) +32*j,'%Y-%m-01')),
          "liq" = temp_liq,
          "comment" = "Inserted")
        ndakota = rbindlist(list(ndakota, temp_dt))
      }
    }
  }
}

toc_missing = Sys.time()
time_usage_missing = toc_missing - tic_missing
time_usage_missing
ndakota_missing = ndakota
# ndakota = ndakota_missing
write.csv(ndakota_missing, 'ndakota_missing.csv', row.names = F)


##project 15 month forward ######################################################################################################
###

# ndakota = as.data.table(ndakota)
setkey(ndakota, entity_id, basin, first_prod_year)
ndakota[, last_prod_date:= as.character(last_prod_date)]
ndakota[, prod_date:= as.character(prod_date)]
# sapply(ndakota, class)

### define function to predict production.
forward_liq_func <- function(j){
  temp <- forward_dt[j, ]
  temp_entity_id <- temp[,entity_id]
  temp_basin <- temp[, basin]
  
  
  if (temp[, first_prod_year] < 1980) {
    first <- 1980 } else {
      first <- temp[, first_prod_year]
    }
    dcl_mth <- (max_n_mth + 1)
    temp_dt = data.table( "liq"= round(temp[,liq] * exp(dcl[(first_prod_year == first & n_mth == dcl_mth  & basin == temp_basin), avg]/100),0))
  return(temp_dt$liq)
}

### Setting clusters for parallel computing.
numOfWorkers = 3
cl <- makeCluster(numOfWorkers)
registerDoParallel(cl)
# stopCluster(cl) # After making forward projection, cluster must be stopped.
###

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
                   where avg >= 20")
  
  forward = as.data.table(forward)
  # head(forward)
  
  ndakota_last = ndakota[last_prod_date == prod_date, ]
  forward_dt = ndakota_last[entity_id %in% forward[, entity_id], ]
  
  # Making forward projection.
  # Cluster need to be set before.
  forward_liq_proj = foreach(j = 1:nrow(forward_dt), .combine = c, .packages = 'data.table') %dopar%
    forward_liq_func(j)
  
  
  ### Except liq and last_prod_date, change other columns outside the long iterations. 
      ## entity_id, basin, first_prod_year remain the same.
      ## Only update values for last_prod_date, n_mth, prod_date, comment
  temp_ndakota_forward = forward_dt
  temp_ndakota_forward[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_forward[, n_mth:= (n_mth + 1)]
  temp_ndakota_forward[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_ndakota_forward[, comment:= "Inserted"]
  temp_ndakota_forward[, liq:= forward_liq_proj]
  ###
  
  # Update the last_prod_date for all the entities.
  ndakota[entity_id %in% forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  
  # Append the forward prediction in original data set.
  ndakota = rbindlist(list(ndakota, temp_ndakota_forward))
  setkey(ndakota, entity_id, basin, first_prod_year)
  cat(sprintf('Iteration %i runs successfully...\n', i))
}

###
# After making forward projection, cluster must be stopped.
stopCluster(cl) 
###

write.csv(ndakota,"C:/Users/Xiao Wang/Desktop/Programs/Projects/Prod_CO_WY/Samantha_Code/ndakota_forward.csv", row.names = F)

toc_forwad = Sys.time()
time_usage_forward = toc_forwad - tic_forwad
write.csv(time_usage_forward,"C:/Users/Xiao Wang/Desktop/time_uasge.csv")

## forward prediction for new production #########################################################################################################

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


##transform future price table
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

## calculate the moving average
Moving_Avg <- function(data, interval){
  n = length(data)
  m = interval
  SMA = data.frame('SMA' = rep(0, (n - m + 1)))
  
  for(i in n:1){
    if((i - m + 1) <= 0){
      break
    } else{
      avg <- mean(data[i: (i - m + 1)])
      j = (n - i + 1)
      SMA[j,1] = avg
    }
  }
  SMA = SMA[c((n - m + 1):1),]
  
  return(as.data.frame(SMA))
}



# prod of new wells
new_prod <- dbGetQuery(dev_base, "select first_prod_date, extract('year' from first_prod_date) first_prod_year, 
                       extract('month' from first_prod_date) first_prod_month, 
                       round(sum(liq)/1000/(extract(days from (first_prod_date + interval '1 month' - first_prod_date))),0) as new_prod
                       from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                       where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 and state = 'ND' 
                       and first_prod_date < date_trunc('month', current_date)::DATE - interval '5 month' and first_prod_date = prod_date
                       group by 1,2,3
                       order by 1,2,3")

new_prod$first_prod_date <- as.Date(new_prod$first_prod_date)




## prod

hist_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                        from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                        where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 
                        and state = 'ND' and prod_date < date_trunc('month', current_date)::DATE - interval '5 month'
                        group by prod_date
                        order by 1")

hist_prod$prod_date <- as.Date(hist_prod$prod_date)

## forward prod from hist wells
prod <-  plyr::ddply(ndakota, 'prod_date', summarise, sum = sum(liq)/1000)

prod <- mutate(prod, prod = sum/as.numeric(as.Date(format(as.Date(prod_date+32),'%Y-%m-01')) - prod_date, units = c("days")))

updated_prod <- prod[prod$prod_date > max(hist_prod$prod_date),-2]

first_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                         from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                         where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 
                         and state = 'ND' and prod_date >= date_trunc('month', current_date)::DATE - interval '5 month' and prod_date = first_prod_date
                         group by prod_date
                         order by 1")

new_first_prod <- as.data.frame(matrix(nrow = 15, ncol = 2));
colnames(new_first_prod)<-c('prod_date', 'prod');

## prod from new wells and 15 month forward


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
    new_prod[n+1,1] <- as.character(format(as.Date(new_prod$first_prod_date[n]+32),'%Y-%m-01')) 
    new_prod[n+1,2] <- new_prod$first_prod_year[n] 
    new_prod[n+1,3] <- new_prod$first_prod_month[n] + 1
    new_prod[n+1,4]<- round(predict(lm, data),0)
    
    # new first month production
    new_first_prod[i,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01'))
    new_first_prod[i,2] <- round(predict(lm, data),0)
    
    
    # update the updated production
    temp <- new_first_prod[i,]
    
    for (j in 1:20) {
      if(as.Date(format(as.Date(temp$prod_date)+32*j,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
      {
        break
      }
      if(as.Date(format(as.Date(temp$prod_date)+32*j,'%Y-%m-01'))<= as.Date(max(prod$prod_date)))
      {
        m = nrow(temp)
        temp[m+1, 1] <- as.character(format(as.Date(temp$prod_date[j])+32,'%Y-%m-01'))
        
        if(j < max(dcl$n_mth[dcl$first_prod_year == max(dcl$first_prod_year)])) {
          temp[m+1, 2] <- round(temp$prod[m] * ( 1 + dcl$avg[dcl$first_prod_year == max(dcl$first_prod_year) & dcl$n_mth == (j+1)]/100),0)
        } else {
          temp[m+1, 2] <- round(temp$prod[m] * ( 1 + dcl$avg[dcl$first_prod_year == max(dcl$first_prod_year) & dcl$n_mth == max(dcl$n_mth[dcl$first_prod_year == max(dcl$first_prod_year)])]/100),0)
        }
      }
    }
    
    temp$prod_date <- as.Date(temp$prod_date)
    
    updated_prod <- sqldf("select a.prod_date, a.prod + coalesce(b.prod, 0) as prod
                          from updated_prod a left join temp b on a.prod_date = b.prod_date")
    
    
    ## new total production
    hist_prod[n+1,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01')) 
    hist_prod[n+1,2] <- round(updated_prod$prod[updated_prod$prod_date == hist_prod$prod_date[(n+1)]]
                              - if (length(first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]) == 0) {
                                0
                              } else {
                                first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]
                              }, 0)
    
  }
  
  
}








