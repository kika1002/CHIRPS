
# Load libraries ----------------------------------------------------------
require(pacman)
pacman::p_load(raster, rgdal, rgeos, tidyverse, sf, velox, gtools, velox, foreach, parallel, doSNOW)

# Load Data ---------------------------------------------------------------
g <- gc(reset = TRUE)
rm(list = ls())
options(scipen = 999)

# Functions to use --------------------------------------------------------
extractStt <- function(st, yr){
  # st <- nms[1]; yr <- 1996
  st <- stt[stt@data$name_stt == st,]
  fl <- grep(yr, fls, value = TRUE)
  stk <- stack(fl)
  vls <- raster::extract(stk, st)
  vls <- as.data.frame(vls) %>% 
    mutate(stt = st@data$name_stt) %>% 
    as_tibble() %>% 
    gather(var, value, -stt)
  vl2 <- vls %>% 
    separate(col = var, into = c('name', 'date'), sep = 'y_') %>%
    mutate(date = ymd(date),
           year = year(date),
           mnth = month(date),
           day = day(date)) %>% 
    dplyr::select(stt, date, year, mnth, day, value)
  print('Done!')
  return(vl2)
}

# Load data ---------------------------------------------------------------
stt <- shapefile('shp/stt_ppt.shp')
yrs <- 1995:2015
fls <- list.files('tif', full.names = TRUE, pattern = '.tif') %>% 
  mixedsort() %>% 
  grep(paste0(yrs, collapse = '|'), ., value = TRUE)
nms <- stt@data$name_stt

# Apply the function ------------------------------------------------------
dfm <- lapply(1:length(nms), function(x){
  lapply(1:length(yrs), function(y){
    extractStt(st = nms[x], yr = yrs[y])  
  })
})

nCores <- detectCores(all.tests = FALSE, logical = TRUE)
cl <- makeCluster(nCores - 1)
registerDoSNOW(cl)
dfm <- foreach(x = 1:length(nms), .packages = c('raster', 'rgdal', 'tidyverse', 'lubridate', 'gtools', 'rgeos', 'foreach', 'parallel'), .verbose = TRUE) %dopar% {
  foreach(y = 1:length(yrs)) %do% {
    extractStt(st = nms[x], yr = yrs[y])  
  }
} 
stopCluster(cl)
dfm <- readRDS('dfm.rds')
df <-  flatten(dfm) %>% 
  bind_rows() %>% 
  filter(year %in% 1995:2015)
write.csv(df, 'table_ppt.csv', row.names = F)







