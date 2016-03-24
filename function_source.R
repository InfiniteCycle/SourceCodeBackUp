## @@@ 1st Function
## Define a function to match the decline rate.
## 'dcl' table must be loaded.
find_dcl_factor = function(basin_, first_, month_){
  # Find the log decline rate first.
  dcl_rate = dcl[(first_prod_year == first_ & basin == basin_) & n_mth == month_, avg]/100
  # the decline rate is calculated as r(t) = log(1 + P(t)) - log(1 + P(t - 1))
  # the decline rate factor is calculated as 10^dcl)
  # The next period production is: P(t) = (1 + P(t - 1))*10^(dcl) -1
  dcl_factor = 10^(dcl_rate)
  return(dcl_factor)
}

## @@ 2nd Function
## Define a function for changing data type. Char --> Date
toDate <- function(date_char, j){
  date_res <- as.Date(format(as.Date(date_char) + 32*j, '%Y-%m-01'))
  return(date_res)
}

## @@ 3th Function
## Define a function for changing data type. Date --> Char
toChar <- function(date_real, j){
  char_res <- as.character(format(as.Date(date_real)+32*j,'%Y-%m-01'))
  return(char_res)
}

## @@ 4th Function
## Define a function to make forward projection
## This function could be executed parallelly.
forward_liq_func <- function(j){
  temp <- forward_dt[j, ]
  temp_entity_id <- temp[,entity_id]
  temp_basin <- temp[, basin]

  if (temp[, first_prod_year] < 1980) {
    first <- 1980 }
  else {
    first <- temp[, first_prod_year]
  }

  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- basin_max_mth_tbl[basin == temp_basin & first_prod_year == first, max]

  ## Actual max month of production of the entity
  max_n_mth <- temp[prod_date == last_prod_date[1], n_mth]

  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth + 1
  } else {
    dcl_mth <- max_n_mth + 1
  }

  # dcl_mth <- (max_n_mth + 1)
  temp_dt = data.table("liq"= round((1 + temp[,liq]) * find_dcl_factor(temp_basin, first, dcl_mth) - 1, 0))
  return(temp_dt$liq)
}

## @@ 5th Function
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

## @@ 6th Function
### The function will return a data.table containing basin DI.
partition_load <-function(tbl_name, part_num){
  ### tbl_name: the large table to extract basin data.
  ### tbl_name should be like: dev.zsz_permian_dec
  ### part_num: how many parts need to be partitioned.

  ## load required library first.
  library(pipeR) # if loaded before, this line can be commented.
  library(data.table)
  ###

  first_prod_year <- sprintf("select distinct(first_prod_year) from %s
                             order by first_prod_year", tbl_name) %>>%
                             {dbGetQuery(base, .)}
  n = nrow(first_prod_year)
  PARTNUM <- part_num
  ind <- seq(from = 1, to = n,  by = (n - 1)/PARTNUM) # partition the table into 5 parts
  thres <- first_prod_year[ind[2:PARTNUM],] # threshold for making partitions.

  # create an empty data table to hold all values.
  result_table <- sprintf("select * from %s limit 0", tbl_name) %>>%
                          {dbGetQuery(base, .)} %>>%
                          {as.data.table(.)}

  for(i in 1:(PARTNUM)){
    if(i == 1){
      query <- sprintf("select * from %s
                       where first_prod_year < %s", tbl_name, thres[i])
    } else if(i == (PARTNUM)){
      query <- sprintf("select * from %s
                       where first_prod_year >= %s", tbl_name, thres[i - 1])
    } else{
      query <- sprintf("select * from %s where first_prod_year >= %s
                       and first_prod_year < %s", tbl_name,
                       thres[i - 1], thres[i])
    }

    ## using data.table and rbindlist for fast table binding.
    partition <- dbGetQuery(base, query) %>>% as.data.table()
    result_table <- rbindlist(list(result_table, partition))
  }
  return(result_table)
}


## @@ 7th Function


