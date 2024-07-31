require(dplyr)
require(tidyr)
require(tidyverse)
require(foreach)
require(doParallel)
require(parallel)
devtools::install_github("https://github.com/EduardoFernandezC/dormancyR")
require(dormancyR)
require(dplyr)
require(tidyr)
require(tidyverse)
require(pracma)
require(data.table)
require(weathermetrics)
require(measurements)
require(naniar)
require(dormancyR)
require(chron)
require(ggrepel)
require(geosphere)
require(chillR)
require(readr)
require(fruclimadapt)
require(parallel)
require(pbapply)
require(whereami)
require(dtplyr)

current_file_path <- whereami()
current_file_dir <- dirname(current_file_path)
setwd(current_file_dir)



#GHCN-Daily stations

download.file('https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt', 'ghcnd-stations.txt')
download.file('https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt', 'ghcnd-inventory.txt')
options(timeout = 300)

#Inventory file and station file
stns = read_table('ghcnd-stations.txt',col_names = F)
colnames(stns) = c('ID'	,'LAT',	'LON',	'ELEV',	'ST',	'NAME',	'GSN',	'HCN',	'WMOID')
inv <- read_table('ghcnd-inventory.txt',col_names = F)
colnames(inv) = c('ID',	'LAT',	'LON',	'ELEM',	'FIRST',	'LAST')

load('Cultivars_NYUS_2_1.Rdata')

today_date = as.Date('2024-06-01')

#Identify usable stations with same number of years of Tmin and Tmax
usable_stations = filter(inv, ELEM %in% c('TMIN','TMAX'))
usable_stations_1 = usable_stations %>%
  group_by(ID) %>%
  mutate(n_year = LAST - FIRST) %>%
  summarise(n=n(),
            n_n_year = n_distinct(n_year)) %>%
  filter(! n < 2,! n_n_year >1)

usable_stations = filter(usable_stations,ID %in% usable_stations_1$ID)

#North America stations

usable_stations = filter(usable_stations, LAT >= 30 & LAT <= 55 & LON >= -127 & LON <= -60)

#The station has record for the current year
usable_stations = filter(usable_stations,LAST == 2024, FIRST <= 1999)

#The stations are in the US or Canada
usable_stations = usable_stations %>%
  filter(grepl("^US", ID) | grepl("^CA", ID))

stns_subset <- stns %>%
  select(ID, ST)

usable_stations <- usable_stations %>%
  left_join(stns_subset, by = "ID")

usable_stations = unique(usable_stations$ID)

urls = paste0('https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/',usable_stations,'.csv')

# Function to download and read a CSV file (for each weather station) from a URL and process the data
read_csv_from_url <- function(url) {
  library(readr)
  library(tidyverse)
  
  # Error handling
  tryCatch({
    data <- read_csv(url, progress = FALSE, show_col_types = FALSE)
    data <- data[, which(colnames(data) %in% c("STATION", "DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMIN"))]
    data$DATE <- as.Date(data$DATE)
    data$TMAX <- data$TMAX / 10
    data$TMIN <- data$TMIN / 10
    data <- filter(data, DATE >= as.Date('1999-08-10') & DATE <= today_date, !is.na(TMAX), !is.na(TMIN))
    colnames(data) <- c("ID", "Date", "lat", "lon", "Tmax", "Tmin")
    return(data)
  }, error = function(e) {
    # Print the error message and URL that caused the error
    message("Error in processing URL: ", url)
    message("Error message: ", e)
    # Return NULL to indicate the error
    return(NULL)
  })
}


# Read all CSV files and store them in a list. This process include parallel processing
no_cores <- detectCores(logical = TRUE)
cl <- makeCluster(no_cores-2)  
clusterExport(cl, 'urls')
clusterExport(cl, 'read_csv_from_url')
clusterExport(cl, 'today_date')
#clusterEvalQ(cl, library(readr))

#Data download
data_list <- pblapply(urls, read_csv_from_url,cl = cl)
data_list <- data_list[!sapply(data_list, is.null)]

stopCluster(cl)



#Helper functions
#feature_extraction for model prediction
Weather_feature_generation = function(df){
  
  library(chillR)
  library(dormancyR)
  library(dplyr)
  library(pracma)
  library(lubridate)
  library(fruclimadapt)
  
  
  inverse_EWA = function(datalist,window=10){
    datalist_rev = rev(datalist)
    datalist_rev_EWA = movavg(datalist_rev,n = window,type = 'e')
    datalist_EWA = rev(datalist_rev_EWA)
    datalist_EWA = c(rep(NA,window), datalist_EWA)
    datalist_EWA = datalist_EWA[1:length(datalist)]
    return(datalist_EWA)
  }
  
  
  
  df$Date<- as.Date(df$Date)
  df <- df %>% 
    arrange(Date)
  
  df = filter(df, !is.na(Tmin), !is.na(Tmax))
  df$Year <- as.numeric(format(df$Date, format = "%Y"))
  df$Month <-  as.numeric(format(df$Date, format = "%m"))
  df$Day <-  as.numeric(format(df$Date, format = "%d"))
  df <- filter(df, Month %in% c(1,2,3,4,8,9,10,11,12))
  df <-df %>%
    group_by(Year) %>%
    mutate(ind = sum(is.na(Tmin))) %>%
    group_by(Year) %>%
    filter(!any(ind >= 10)) %>%
    select(-ind)
  #colnames(df)[1:2] <- c('Tmin','Tmax')
  df <- filter(df, !is.na(Tmax) | is.na(Tmin), !is.na(Tmax) & !is.na(Tmin),!Tmax > 100, !Tmin < -100, !Tmin > Tmax)
  
  if (nrow(df) < 21) {
    df = NULL
  }
  
  else
  {
    
    
    data_all_hourly = stack_hourly_temps(df,latitude = df$lat[1])[[1]]
    data_all_hourly$DOY = yday(data_all_hourly$Date)
    data_all_hourly$Date = data_all_hourly$Date
    data_all_hourly_1 = data.frame(Year = data_all_hourly$Year,
                                   Month = data_all_hourly$Month,
                                   Day = data_all_hourly$Day,
                                   DOY = data_all_hourly$DOY,
                                   Hour = data_all_hourly$Hour,
                                   Temp = data_all_hourly$Temp)
    #chilling_computation
    CU <- chilling_units(data_all_hourly$Temp, summ = F)
    Utah<- modified_utah_model(data_all_hourly$Temp, summ = F)
    NC<-north_carolina_model(data_all_hourly$Temp, summ = F)
    GDH_10 <- GDH_linear(data_all_hourly_1, Tb = 10, Topt = 25, Tcrit = 36)
    GDH_7 <- GDH_linear(data_all_hourly_1, Tb = 7, Topt = 25, Tcrit = 36)
    GDH_4 <- GDH_linear(data_all_hourly_1, Tb = 4, Topt = 25, Tcrit = 36)
    GDH_0 <- GDH_linear(data_all_hourly_1, Tb = 0, Topt = 25, Tcrit = 36)
    DP <- Dynamic_Model(data_all_hourly$Temp, summ = F)
    
    
    CU = if_else(CU < 0, 0, CU)
    Utah = if_else(Utah < 0, 0 ,Utah)
    NC = if_else(NC < 0,0, NC)
    
    All_chilling_data <- data.frame(Date = data_all_hourly$Date,
                                    Month = format(data_all_hourly$Date,format = "%b"),
                                    Year = format(data_all_hourly$Date,format = "%Y"),
                                    CU = CU,
                                    Utah = Utah,
                                    NC = NC,
                                    DP = DP)
    
    All_chilling_data$Year  = as.numeric(All_chilling_data$Year)
    
    All_chilling_data$CU1 <- ifelse(All_chilling_data$Month %in% c('Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr'),All_chilling_data$CU, 0 )
    All_chilling_data$Utah1 <- ifelse(All_chilling_data$Month %in% c('Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr'),All_chilling_data$Utah, 0 )
    All_chilling_data$NC1 <- ifelse(All_chilling_data$Month %in% c('Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr'),All_chilling_data$NC, 0 )
    GDH_10$GDH <- ifelse(GDH_10$Month %in% c(1,2,3,4),GDH_10$GDH, 0 )
    GDH_7$GDH <- ifelse(GDH_7$Month %in% c(1,2,3,4),GDH_7$GDH, 0 )
    GDH_4$GDH <- ifelse(GDH_4$Month %in% c(1,2,3,4),GDH_4$GDH, 0 )
    GDH_0$GDH <- ifelse(GDH_0$Month %in% c(1,2,3,4),GDH_0$GDH, 0 )
    All_chilling_data$DP1 <- ifelse(All_chilling_data$Month %in% c('Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr'),All_chilling_data$DP, 0 )
    
    
    
    All_chilling_data$dormant_season <- ifelse(All_chilling_data$Month %in% c('Sep','Oct','Nov','Dec'), paste0(All_chilling_data$Year,'-',All_chilling_data$Year + 1),
                                               ifelse(All_chilling_data$Month %in% c('Jan','Feb','Mar','Apr'),paste0(All_chilling_data$Year -1,'-',All_chilling_data$Year),NA))
    
    All_chilling_data <- All_chilling_data[order(All_chilling_data$Date),]
    
    
    
    Chilling_data_summary <-  All_chilling_data %>%
      group_by(dormant_season) %>%
      summarise(Utah= cumsum(Utah1),
                CU = cumsum(CU1),
                NC = cumsum(NC1),
                #GDD_10 = cumsum(GDD_10_1),
                #GDD_7 = cumsum(GDD_7_1),
                #GDD_4 = cumsum(GDD_4_1),
                #GDD_0 = cumsum(GDD_0_1),
                DP = cumsum(DP1),
                Date = Date)
    
    
    GDHs = data.frame(Date = as.Date(GDH_10$Date),
                      GDH10_1 = GDH_10$GDH,
                      GDH_7_1 = GDH_7$GDH,
                      GDH_4_1 = GDH_4$GDH,
                      GDH_0_1 = GDH_0$GDH)
    #GDHs <- filter(GDHs,!format(GDHs$Date,format = "%b") == 'Aug') 
    
    GDHs = GDHs %>%
      mutate(season = ifelse(format(GDHs$Date,format = "%b") %in% c('Sep','Oct','Nov','Dec'), 
                             paste0(as.numeric(format(GDHs$Date,format = "%Y")),"-",as.numeric(format(GDHs$Date,format = "%Y")) + 1),
                             paste0(as.numeric(format(GDHs$Date,format = "%Y")) - 1,"-",as.numeric(format(GDHs$Date,format = "%Y"))))) %>%
      group_by(season) %>%
      summarise(GDH_10 =cumsum(GDH10_1),
                GDH_7 =cumsum(GDH_7_1),
                GDH_4 = cumsum(GDH_4_1),
                GDH_0 = cumsum(GDH_0_1),
                Date = Date) %>%
      arrange(Date) %>%
      select(-season)
    
    GDHs = GDHs[,-c(1,6)]
    
    
    Chilling_data_summary_daily <-  Chilling_data_summary %>%
      group_by(Date) %>%
      summarise(
        CU = max(CU),
        NC = max(NC),
        Utah = max(Utah),
        #GDD_10 = max(GDD_10),
        #GDD_7 = max(GDD_7),
        #GDD_4 = max(GDD_4),
        #GDD_0 = max(GDD_0),
        DP = max(DP)) %>%
      arrange(Date)
    
    Chilling_data_summary_daily <- bind_cols(Chilling_data_summary_daily,GDHs)
    
    
    #Daily_T
    data_all_daily_T <-  data_all_hourly  %>%
      group_by(Date) %>%
      summarise(ID = ID[1],
                lat = lat[1],
                lon = lon[1],
                #ST = State[1],
                Year = mean(Year),
                Month = mean(Month),
                Day = mean(Day),
                min_temp = min(Temp),
                max_temp = max(Temp),
                within_day_range_temp = max(Temp) - min(Temp),
                Naive_average_temp = mean(Temp),
                Median_temp = median(Temp))
    
    #reverse EWAs
    
    window_size = c(2,4,6,8,10,12,14,16,18,20)
    
    #min
    min_data_number = length(inverse_EWA(data_all_daily_T$min_temp, window = window_size[2]))
    min_REWA = data.frame(NA_col = rep(NA, min_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('min_REWMA_',window_size[i],"day"))
      #assign(vname, inverse_EWA(data_all_daily_T$min_temp, window = window_size[i]))
      #col = inverse_EWA(data_all_daily_T$min_temp, window = window_size[i])
      min_REWA[,i] = inverse_EWA(data_all_daily_T$min_temp, window = window_size[i])
      colnames(min_REWA)[i] = vname
    }
    
    #max
    max_data_number = length(inverse_EWA(data_all_daily_T$max_temp, window = window_size[2]))
    max_REWA = data.frame(NA_col = rep(NA, max_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('max_REWMA_',window_size[i],"day"))
      #assign(vname, inverse_EWA(data_all_daily_T$max_temp, window = window_size[i]))
      #col = inverse_EWA(data_all_daily_T$max_temp, window = window_size[i])
      max_REWA[,i] = inverse_EWA(data_all_daily_T$max_temp, window = window_size[i])
      colnames(max_REWA)[i] = vname
    }
    
    #mean
    mean_data_number = length(inverse_EWA(data_all_daily_T$Naive_average_temp, window = window_size[2]))
    mean_REWA = data.frame(NA_col = rep(NA, mean_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('mean_REWMA_',window_size[i],"day"))
      #assign(vname, inverse_EWA(data_all_daily_T$mean_temp, window = window_size[i]))
      #col = inverse_EWA(data_all_daily_T$mean_temp, window = window_size[i])
      mean_REWA[,i] = inverse_EWA(data_all_daily_T$Naive_average_temp, window = window_size[i])
      colnames(mean_REWA)[i] = vname
    }
    
    
    #EWMAs
    
    #min
    min_data_number = length(movavg(data_all_daily_T$min_temp, n = window_size[2],type = 'e'))
    min_EWA = data.frame(NA_col = rep(NA, min_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('min_EWMA_',window_size[i],"day"))
      min_EWA[,i] = movavg(data_all_daily_T$min_temp, n = window_size[i],type = 'e')
      colnames(min_EWA)[i] = vname
    }
    
    
    #max
    max_data_number = length(movavg(data_all_daily_T$max_temp, n = window_size[2],type = 'e'))
    max_EWA = data.frame(NA_col = rep(NA, max_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('max_EWMA_',window_size[i],"day"))
      max_EWA[,i] = movavg(data_all_daily_T$max_temp, n = window_size[i],type = 'e')
      colnames(max_EWA)[i] = vname
    }
    
    
    #mean
    mean_data_number = length(movavg(data_all_daily_T$Naive_average_temp, n = window_size[2],type = 'e'))
    mean_EWA = data.frame(NA_col = rep(NA, mean_data_number))
    for (i in 1:length(window_size)) {
      vname = print(paste0('mean_EWMA_',window_size[i],"day"))
      mean_EWA[,i] = movavg(data_all_daily_T$Naive_average_temp, n = window_size[i],type = 'e')
      colnames(mean_EWA)[i] = vname
    }
    
    
    
    #Output
    df_out = merge(data_all_daily_T, Chilling_data_summary_daily, by = c('Date'))
    df_out = data.frame(df_out,
                        min_EWA,
                        min_REWA,
                        max_EWA,
                        max_REWA,
                        mean_EWA,
                        mean_REWA,
                        photoperiod = daylength(df_out$lat[1],yday(df_out$Date)))
    
    df_out = filter(df_out, !Month == 8)
  }
  
}

#extracted data processing
autogluon_input_transformation = function(df,cv_of_interest) {
  
  library(dplyr)
  selected_cultivar = cv_of_interest  
  selected_cultivar_colname = paste0('Cultivar.',selected_cultivar)
  
  df <- df[,!colnames(df) %in% c('photoperiod.Sunset','photoperiod.Sunrise')]
  
  #colnames(df)[42] <- 'photoperiod.Daylength'
  
  df$season <-  ifelse(df$Month %in% c(9,10,11,12), paste0(df$Year,'-',df$Year + 1),
                       ifelse(df$Month %in% c(1,2,3,4),paste0(df$Year -1,'-',df$Year),NA))
  
  df <- filter(df,!is.na(season))
  
  df[Cultivars] = 0
  
  col_number = which(colnames(df) == selected_cultivar_colname)
  
  df[,col_number] = 1
  df$Treatment.tetralone.ABA = 0
  return(df)
  
}



#Feature extraction computation
extracted_data <- data_list[!sapply(data_list,is.null)]
no_cores <- detectCores(logical = TRUE)
cl <- makeCluster(no_cores-3)  
clusterExport(cl, 'extracted_data')
feature_extracted_data <- pblapply(cl = cl, extracted_data, Weather_feature_generation)
stopCluster(cl)
gc()

feature_extracted_data <- feature_extracted_data[!sapply(feature_extracted_data,is.null)]
feature_extracted_data <- feature_extracted_data[!sapply(feature_extracted_data,nrow) == 0]

#The resulting data is ready for autogluon prediction for 'Riesling' cultivar. You can change the cultivar of interest by changing the cv_of_interest parameter in the function below.
weather_feature_files_for_autodgluon <- lapply(feature_extracted_data,FUN = autogluon_input_transformation, cv_of_interest = 'Riesling')


data_from_autogluon_prediction <- weather_feature_files_for_autodgluon %>%
  bind_rows()
data_from_autogluon_prediction$Days_in_season = as.numeric(data_from_autogluon_prediction$Date - if_else(as.numeric(month(data_from_autogluon_prediction$Date)) %in% c(9,10,11,12), ymd(paste0(year(data_from_autogluon_prediction$Date),"-",9,"-",01)),
                                                                                                                ymd(paste0(year(data_from_autogluon_prediction$Date)-1,"-",9,"-",01))))


write_csv(data_from_autogluon_prediction,'data_for_autogluon_prediction.csv')