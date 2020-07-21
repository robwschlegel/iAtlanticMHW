# code/functions.R
# The purpose of this script is to house functions used with the iAtlantic data


# Setup -------------------------------------------------------------------

library(tidyverse)
library(tidync)
library(heatwaveR)
library(doParallel)
registerDoParallel(cores = 48)
detectCores() # Should be 48

# iMirabilis files
iMirabilis_files <- dir("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis", 
                        full.names = T, pattern = "_T_")[8:56] # Some late 60's years are missing so we start from 1970
SAMBA_files <- dir("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/SAMBA/", 
                   full.names = T, pattern = "_T_")[7:55]

# Lon/lat values
iMirabilis_lonlat <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_iMirabilis.nc") %>% 
  activate("D1,D0") %>% 
  hyper_tibble()
SAMBA_lonlat <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_SAMBA.nc") %>% 
  activate("D1,D0") %>% 
  hyper_tibble()

# Land masks
iMirabilis_mask <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_iMirabilis.nc") %>% 
  hyper_tibble() %>% 
  filter(tmask == 1) %>% 
  select(tmask, x, y) %>% 
  unique() %>% 
  mutate(row_index = 1:n())
SAMBA_mask <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_SAMBA.nc") %>% 
  hyper_tibble() %>% 
  filter(tmask == 1) %>% 
  select(tmask, x, y) %>% 
  unique() %>%  
  mutate(row_index = 1:n())

# Plot the mask
# left_join(SAMBA_mask, SAMBA_lonlat) %>% 
#   ggplot(aes(x = nav_lon, y = nav_lat)) +
#   borders() +
#   geom_tile(aes(fill = as.factor(tmask))) +
#   coord_cartesian(xlim = c(min(SAMBA_lonlat$nav_lon)-2, max(SAMBA_lonlat$nav_lon)+2),
#                   ylim = c(min(SAMBA_lonlat$nav_lat)-2, max(SAMBA_lonlat$nav_lat)+2))


# Load --------------------------------------------------------------------

# ncdump::NetCDF("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/SAMBA/comressed_1_INALT20.L46-KFS101_1d_19580101_19581231_grid_T_SAMBA.nc")
# ncdump::NetCDF("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS104_1d_19940101_19941231_grid_T_iMirabilis.nc")
# tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS101_1d_19580101_19581231_grid_T_iMirabilis.nc")

# NB: This file appears to be broken
# ncdf4::nc_open("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS104_1d_19940101_19941231_grid_T_iMirabilis.nc")
# file.info("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS104_1d_19940101_19941231_grid_T_iMirabilis.nc")
# file.info("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS104_1d_19950101_19951231_grid_T_iMirabilis.nc")


# testers...
# x_sub <- 24
# file_names <- SAMBA_files
# file_names <- iMirabilis_files
# file_name <- file_names[25]

# Load a subset of a single file
load_iAtlantic_sub <- function(file_name, x_sub){
  res_sub <- tidync(file_name) %>% 
    hyper_filter(x = x == x_sub) %>% 
    hyper_tibble(select_var = "votemper") %>% 
    dplyr::rename(temp = votemper, t = time_counter) %>% 
    filter(temp != 0) %>% 
    mutate(t = as.Date(as.POSIXct(t, origin = "1900-01-01")))
}

# Load the same subsets for all files
process_iAtlantic <- function(lon_x, file_names){
  res <- plyr::ldply(file_names, load_iAtlantic_sub, .parallel = T, x_sub = lon_x)
  return(res)
}

# Test on one lon x
# NB: 1 iMirabilis file is currently broken
# system.time(iMirabilis_test <- load_iAtlantic_files(13, iMirabilis_files[-25])) # 84 seconds
# system.time(SAMBA_test <- process_iAtlantic(24, SAMBA_files)) # 42 seconds


# Detect ------------------------------------------------------------------

# This function detects the events in the processed iAtlantic data
# It returns a heavily cleaned up set of results in the interest of storage space
# testers...
# df <- SAMBA_test
detect_event_iAtlantic <- function(df){
  # Get the base of the results
  system.time(
  res_base <- df %>% 
    group_by(x, y, deptht) %>% 
    nest() %>% 
    mutate(clim = purrr::map(data, ts2clm, climatologyPeriod = c("1981-01-01", "2010-12-31")),
           event = purrr::map(clim, detect_event),
           cat = purrr::map(event, category, season = "peak", climatology = FALSE)) %>% 
    dplyr::select(-data, -clim)
  ) # 26 seconds for one full SAMBA x slice
  
  # Pull out the event data
  res_event <- res_base %>% 
    dplyr::select(-cat) %>% 
    unnest(event) %>% 
    filter(row_number() %% 2 == 0) %>% 
    unnest(event) %>% 
    dplyr::select(x:event_no, duration:intensity_max, intensity_cumulative, rate_onset, rate_decline)
  
  # Pull out the category data
  res_cat <- res_base %>% 
    dplyr::select(-event) %>% 
    unnest(cat) %>% 
    dplyr::select(x:event_name, category, p_moderate:season)
 
  # Join results and exit
  res_full <- left_join(res_event, res_cat, by = c("x", "y", "deptht", "event_no"))
  return(res_full)
}


# Pipeline ----------------------------------------------------------------

# The code in this section is used to run the full detection pipeline
pipeline_iAtlantic <- function(lon_steps, file_names){
  
  
}

# SAMBA run
pipeline_iAtlantic(min(SAMBA_mask$x):max(SAMBA_mask$x), SAMBA_files)


# Visualise ---------------------------------------------------------------

# And finally some visualisations of the results

