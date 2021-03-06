# code/functions.R
# The purpose of this script is to house functions used with the iAtlantic data
# Currently these functions are called from "~/iAtlantic_pipeline.R",
# which is not within this project due to SLURM root direcotry convenience


# Setup -------------------------------------------------------------------

library(tidyverse)
library(tidync)
library(heatwaveR)
library(doParallel)
registerDoParallel(cores = 48)
paste0("Cores: ", detectCores()) # Should be 48

## NB: For some reason SLURM does not like the dir function
## So we save the file directories as RData objects to be loaded later
# iMirabilis file names
# iMirabilis_files <- dir("/share/projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis", 
#                         full.names = T, pattern = "_T_")
# save(iMirabilis_files, file = "/share/people/3503570/iAtlanticMHW/metadata/iMirabilis_files.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/iMirabilis_files.RData")

# SAMBA file names
# SAMBA_files <- dir("/share/projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/SAMBA", 
#                    full.names = T, pattern = "_T_")
# save(SAMBA_files, file = "/share/people/3503570/iAtlanticMHW/metadata/SAMBA_files.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/SAMBA_files.RData")

# Lon/lat values
# iMirabilis_lonlat <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_iMirabilis.nc") %>% 
#   activate("D1,D0") %>% 
#   hyper_tibble()
# save(iMirabilis_lonlat, file = "metadata/iMirabilis_lonlat.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/iMirabilis_lonlat.RData")
# SAMBA_lonlat <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_SAMBA.nc") %>% 
#   activate("D1,D0") %>% 
#   hyper_tibble()
# save(SAMBA_lonlat, file = "metadata/SAMBA_lonlat.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/SAMBA_lonlat.RData")

# Land masks
# iMirabilis_mask <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_iMirabilis.nc") %>% 
#   hyper_tibble() %>% 
#   filter(tmask == 1) %>% 
#   select(tmask, x, y) %>% 
#   unique() %>% 
#   mutate(row_index = 1:n())
# save(iMirabilis_mask, file = "metadata/iMirabilis_mask.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/iMirabilis_mask.RData")
# SAMBA_mask <- tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/mesh_mask/mesh_mask_SAMBA.nc") %>% 
#   hyper_tibble() %>% 
#   filter(tmask == 1) %>% 
#   select(tmask, x, y) %>% 
#   unique() %>%  
#   mutate(row_index = 1:n())
# save(SAMBA_mask, file = "metadata/SAMBA_mask.RData")
load("/share/people/3503570/iAtlanticMHW/metadata/SAMBA_mask.RData")

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

# testers...
# x_sub <- 20
# file_names <- SAMBA_files
# file_names <- iMirabilis_files
# file_name <- file_names[1]

# Load a subset of a single file
load_iAtlantic_sub <- function(file_name, x_sub){
  res_sub <- tidync(file_name) %>% 
    hyper_filter(x = x == x_sub) %>% 
    hyper_tibble(select_var = "votemper") %>% 
    dplyr::rename(temp = votemper, t = time_counter) %>% 
    filter(temp != 0) %>% 
    mutate(t = as.Date(as.POSIXct(t, origin = "1900-01-01")))
  return(res_sub)
}

# Test run for one lon x
# system.time(iMirabilis_test <-  plyr::ldply(iMirabilis_files, load_iAtlantic_sub, .parallel = T, x_sub = 13)) # 487 seconds
# system.time(SAMBA_test <- plyr::ldply(SAMBA_files, load_iAtlantic_sub, .parallel = T, x_sub = 24)) # 42 seconds


# Detect ------------------------------------------------------------------

# testers...
# df <- SAMBA_test %>%
#   filter(x == 24, y == 9, deptht < 3.1) %>% 
#   dplyr::select(-x, -y, -deptht)

# This function detects the events in the processed iAtlantic data
# It returns a heavily cleaned up set of results in the interest of storage space
detect_event_iAtlantic <- function(df){
  # Get the base of the results
  # system.time(
  res_base <- df %>% 
    nest(data = everything()) %>% 
    mutate(clim = purrr::map(data, ts2clm, climatologyPeriod = c("1981-01-01", "2010-12-31")),
           event = purrr::map(clim, detect_event),
           cat = purrr::map(event, category, season = "peak", climatology = FALSE)) %>% 
    dplyr::select(-data, -clim)
  # ) # 26 seconds for one full SAMBA x slice
  
  # Pull out the event data
  res_event <- res_base %>% 
    dplyr::select(-cat) %>% 
    unnest(event) %>% 
    filter(row_number() %% 2 == 0) %>% 
    unnest(event) %>% 
    dplyr::select(event_no, date_start:date_end, duration, intensity_mean, 
                  intensity_max, intensity_cumulative, rate_onset, rate_decline)
  
  # Pull out the category data
  res_cat <- res_base %>% 
    dplyr::select(-event) %>% 
    unnest(cat) %>% 
    dplyr::select(event_no, event_name, category, p_moderate:season)
  
  # Join results and exit
  res_full <- left_join(res_event, res_cat, by = c("event_no"))
  return(res_full)
}

# Test run
# system.time(
# iMirabilis_MHW_test <- plyr::ddply(iMirabilis_test, c("x", "y", "deptht"), detect_event_iAtlantic, .parallel = T)
# ) # 582 seconds
# system.time(
# SAMBA_MHW_test <- plyr::ddply(SAMBA_test, c("x", "y", "deptht"), detect_event_iAtlantic, .parallel = T)
# ) # 4 seconds


# Pipeline ----------------------------------------------------------------

# The code in this section is used to run the full detection pipeline
pipeline_iAtlantic <- function(lon_step, file_names){
  
  # Find region name
  region_name <- str_split(file_names[1], pattern = "_")
  region_name <- sapply(region_name, "[[", length(region_name[[1]]))
  region_name <- sapply(str_split(region_name, pattern = ".nc"), "[[", 1)
  
  # Create four digit value
  lon_step_pad <- str_pad(lon_step, width = 4, pad = "0", side = "left")
  print(paste0("Began run on ",region_name," ",lon_step_pad," at ",Sys.time()))
  
  # Load the data
  # system.time(
  base_data <- plyr::ldply(file_names, load_iAtlantic_sub, .parallel = T, x_sub = lon_step)
  # ) # 42 seconds
  
  # Detect MHWs
  # system.time(
  res_MHW <- plyr::ddply(base_data, c("x", "y", "deptht"), detect_event_iAtlantic, .parallel = T)
  # ) # ~4 to ~20 seconds; depends on depth
  
  # Clean up
  rm(base_data); gc()
  
  # Save
  saveRDS(res_MHW, paste0("/share/people/3503570/iAtlanticMHW/data/",region_name,"/MHW_",region_name,"_",lon_step_pad,".Rds"))
  
  # Clean up
  rm(res_MHW); gc()
  print(paste0("Finished run on ",lon_step_pad," at ",Sys.time()))
}

# SAMBA run
# system.time(
# plyr::l_ply(min(SAMBA_mask$x):max(SAMBA_mask$x), pipeline_iAtlantic, .parallel = F, file_names = SAMBA_files)
# plyr::l_ply(24, pipeline_iAtlantic, .parallel = F, file_names = SAMBA_files)
# ) # ~120 seconds to ~200 seconds for 1 depending on depth

# iMirabilis run
# system.time(
# plyr::l_ply(min(iMirabilis_mask$x):max(iMirabilis_mask$x), pipeline_iAtlantic, file_names = iMirabilis_files)
# plyr::l_ply(1, pipeline_iAtlantic, file_names = iMirabilis_files)
# ) # ~xxx seconds for 1 depending on depth


# Visualise ---------------------------------------------------------------

# And finally some visualisations of the results
# SAMBA_test <- readRDS("/share/people/3503570/iAtlanticMHW/data/SAMBA/MHW_SAMBA_0020.Rds")

