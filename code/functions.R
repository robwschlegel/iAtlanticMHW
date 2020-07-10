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
# ncdump::NetCDF("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS101_1d_19580101_19581231_grid_T_iMirabilis.nc")
# tidync("../../../projects/iAtlantic/INALT20.L46_TIDAL_iAtlantic_AJSmit/iMirabilis/comressed_1_INALT20.L46-KFS101_1d_19580101_19581231_grid_T_iMirabilis.nc")

test_file <- tidync(SAMBA_files[30]) %>% 
  hyper_filter(x = x == 200, y = y == 20) %>% 
  hyper_tibble(select_var = "votemper") %>% 
  dplyr::rename(temp = votemper, t = time_counter) %>% 
  mutate(t = as.Date(as.POSIXct(t, origin = "1900-01-01")))

df <- SAMBA_mask[1,]
x_sub <- df$x
y_sub <- df$y
file_names <- SAMBA_files
file_name <- file_names[1]

# Load a subset of a single file
load_iAtlantic_sub <- function(file_name, x_sub, y_sub){
  res_sub <- tidync(file_name) %>% 
    hyper_filter(x = x == x_sub, y = y == y_sub) %>% 
    hyper_tibble(select_var = "votemper") %>% 
    dplyr::rename(temp = votemper, t = time_counter) %>% 
    filter(temp != 0) %>% 
    mutate(t = as.Date(as.POSIXct(t, origin = "1900-01-01")))
}

# Load the same subsets for all files
load_iAtlantic_files <- function(df, file_names){
  x_sub <- df$x; y_sub <- df$y
  res <- plyr::ldply(file_names, load_iAtlantic_sub, .parallel = T,
                     x_sub = x_sub, y_sub = y_sub)
}

system.time(SAMBA_test <- load_iAtlantic_files(SAMBA_mask[24,], SAMBA_files)) # xxx seconds
system.time(iMirabilis_test <- load_iAtlantic_files(iMirabilis_mask[13,], iMirabilis_files)) # xxx seconds


# Detect ------------------------------------------------------------------



# Visualise ---------------------------------------------------------------

