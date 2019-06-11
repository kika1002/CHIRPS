
# Load libraries ----------------------------------------------------------
require(pacman)
pacman::p_load(raster, rgdal, rgeos, tidyverse, sf, velox, gtools, velox, foreach, parallel, doSNOW, readxl)

# Load Data ---------------------------------------------------------------
g <- gc(reset = TRUE)
rm(list = ls())
options(scipen = 999)
lbl <- data.frame(month_num = 1:12, month = month.abb)

# Functions to use --------------------------------------------------------
tidyTbl <- function(st){
  # st <- stt[1]
  print(paste('To start', st))
  dt <- data %>% 
    filter(stt == st) %>% 
    mutate(mnth = factor(mnth, levels = 1:12)) %>% 
    arrange(year, mnth, day) %>% 
    distinct(stt, date, year, mnth, day, value) %>% 
    mutate(value = round(value, digits = 1))
  print('Quality control')
  vld <- dt %>% 
    group_by(year, mnth) %>% 
    summarize(count = n()) %>% 
    ungroup() %>% 
    mutate(mnth = as.numeric(mnth)) %>% 
    inner_join(., lbl, by = c('mnth' = 'month_num')) %>%
    mutate(month = as.character(month)) %>% 
    inner_join(., bsst, by = c('year' = 'Year', 'month' = 'Month')) %>% 
    mutate(comparison = ifelse(count == Days, TRUE, FALSE)) %>% 
    pull(comparison) %>% 
    unique()
  print('To make the conditional')
  if(vld == T) {
    print('Everything is ok')
    tb <- dt %>% 
      dplyr::select(value) %>% 
      setNames('19950101')
    write.table(tb, paste0('tbl/ppt_', st, '.txt'), row.names = FALSE, col.names = TRUE, quote = FALSE)
  } else {
    warning('There is a problem, please check again')
  }
  print('Done!')
  return(tb)
}

# Load Data ---------------------------------------------------------------
data <- read_csv('table_ppt.csv')
data <- distinct(data, stt, date, year, mnth, day, value)
bsst <- read_excel('bisiestos.xlsx')
stt <- unique(data$stt)

# Apply the functions -----------------------------------------------------
map(.x = stt, .f = tidyTbl)








