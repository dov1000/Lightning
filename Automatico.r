setwd("D:/OneDrive - Instituto Costarricense de Electricidad/Datos_Rayos/")
setwd("C:/Users/daoban/OneDrive - Instituto Costarricense de Electricidad/Datos_Rayos/")
library(getPass)
library(RODBC) 
library(tidyverse)
library(lubridate)

#sc <- spark_connect(master = "local")

#read.csv('Flash2019.txt') 
pass <- "Work21."
BDHidro <- odbcConnect("sihid", uid="daoban", pwd = pass, believeNRows=FALSE)
#funcion usuarios = Crear Tabla de latitud, longitud y fecha de los puntos activos.
usuarios <- function(db){
  SQL_usu <- paste("select pm_id_pm_pk  id,
                  sysdate,
                  pm_latitud,
                  pm_longitud
                   from USRMODESPRD.CAT_PUNTO_MONITOREO_MW 
                  where 
                  pm_estado=1 and pm_tipo=0;
                ", sep = "")
  sqlQuery(db, SQL_usu, dec = ",") %>% 
    as_tibble() %>%
    mutate(Fecha = SYSDATE %>% ymd_hms(),Lat = PM_LATITUD %>% as.character() %>% as.numeric(), Long = PM_LONGITUD %>% as.character() %>% as.numeric()) %>% 
    select(-c(SYSDATE,PM_LATITUD,PM_LONGITUD))
  
}

# consulta =funcion trae descargas fecha, latitud y longitud.
consulta <- function(db){
  
  SQL <- paste("select to_char(des_fecha_hora,'YYYY/MM/DD HH24:MI:SS') FechaActualDescargaEnTexto,
                des_fecha_hora FechaHoraActualDescarga,
                to_char(sysdate,'YYYY/MM/DD HH24:MI:SS') FechaHoraActualConsultaEnTexto,
                --sysdate - interval '3' hour FechaHoraActualDescarga3HorasAntes,
                des_id_descarga_pk ID,
                des_latitud Latitud,
                des_longitud Longitud,
                des_intensidad Intensidad
                from usrmodesprd.dat_descarga_mw
                where des_fecha_hora>(sysdate - interval '3' hour)
                order by des_fecha_hora asc
                ", sep = "")
  
  sqlQuery(db, SQL, dec = ",") %>% 
    as_tibble() %>% 
    mutate(fecha = FECHAHORAACTUALDESCARGA %>% ymd_hms(), Lat = LATITUD , Long = LONGITUD ) %>% 
    select(-c(FECHAHORAACTUALCONSULTAENTEXTO,FECHAACTUALDESCARGAENTEXTO,ID,FECHAHORAACTUALDESCARGA,LATITUD,LONGITUD,INTENSIDAD)) %>% 
    arrange(fecha) 
}


data <- consulta(BDHidro)


data <- read.csv('Flash2019.txt') %>% 
  as_tibble() %>% 
  mutate(fecha = paste(Anno,Mes,Dia,Hora,Min) %>% lubridate::ymd_hm())   %>% 
  select(fecha, Lat, Long, KiloAmp) %>% 
  arrange(fecha) 


#data <- copy_to(sc, data)

gc()

tmax <- 1800
nmin <- 10
dmax <- .05


#data %>% dplyr::mutate(fecha2 = as.numeric(fecha),apply(function(e) deltat = fecha2 - lag(fecha2)))

xx <- data %>% 
  dplyr::mutate(fecha2 = as.numeric(fecha),
                deltat = fecha2 - lag(fecha2),
                deltat = replace_na(deltat,0),
                ind1 = (deltat > tmax),
                cluster1 = cumsum(ind1))  %>% 
  dplyr::group_by(cluster1) %>% 
  dplyr::mutate(n=n()) %>% 
  ungroup() %>% 
  filter(n>nmin) %>% 
  dplyr::group_split(cluster1) %>%
  map(.f = function(df){
    
    res <- cbind(df$Long, df$Lat) %>% 
      dbscan::frNN(eps = dmax) %>% 
      dbscan::dbscan(minPts = nmin)
    
    df$cluster2 <- res$cluster
    rm(res); gc()
    
    df %>% filter(cluster2>0)
  }) %>% 
  bind_rows() %>% 
  dplyr::group_by(cluster1,cluster2) %>% 
  dplyr::mutate(fecha3 = min(fecha2)) %>% 
  ungroup() %>% 
  dplyr::mutate(cluster3 = paste(fecha3, cluster2), 
                cluster4 = factor(cluster3),
                cluster = as.numeric(cluster4)) %>% 
  arrange(cluster) %>% 
  group_by(cluster) %>% 
  mutate(n2 = n()) %>% 
  ungroup() %>% 
  dplyr::select(-fecha2:-ind1, -n:-cluster4) %>% 
  dplyr::mutate(cluster1 = as.factor(cluster1),
                cluster_temporal = as.numeric(cluster1)) %>% 
  dplyr::select(-cluster1)

gc()

yy <- usuarios(BDHidro)

# yy <- xx %>% 
#   dplyr::group_split(cluster) %>% 
#   map(.f = function(df){
#     
#     df <- df %>% arrange(fecha)
#     
#     tt <- tibble(Lat = sample(df$Lat, size = nrow(df)) + runif(nrow(df),-0.001,0.001),
#                  Long = sample(df$Long, size = nrow(df))+ runif(nrow(df),-0.001,0.001),
#                  Fecha = sample(df$fecha,size = nrow(df)) + minutes(floor(runif(nrow(df),-10,10))))
#     
#     tt2 <- tibble(Lat = sample(df$Lat, size = nrow(df)) + runif(nrow(df),-0.1,0.1),
#                   Long = sample(df$Long, size = nrow(df))+ runif(nrow(df),-0.1,0.1),
#                   Fecha = sample(df$fecha,size = nrow(df)) + minutes(floor(runif(nrow(df),-10,10))))
#     
#     tt3 <- tibble(Lat = sample(df$Lat, size = nrow(df)) + runif(nrow(df),-0.01,0.01),
#                   Long = sample(df$Long, size = nrow(df))+ runif(nrow(df),-0.01,0.01),
#                   Fecha = sample(df$fecha,size = nrow(df)) + minutes(floor(runif(nrow(df),-30,30))))
#     
#     tt <- bind_rows(tt,tt2,tt3)
#     
#   }) %>% bind_rows() #%>%  select(-replace)


gc()

#parametros
r1<-3000
r2<-6000
r3<-10000
t1<-10
t2<-20
t3<-30



datos <- tibble(dec0_t1= numeric(length = nrow(yy)),
                dec1_t1= numeric(length = nrow(yy)),
                dec2_t1= numeric(length = nrow(yy)),
                dec3_t1= numeric(length = nrow(yy)),
                dec4_t1= numeric(length = nrow(yy)),
                dec5_t1= numeric(length = nrow(yy)),
                dec6_t1= numeric(length = nrow(yy)),
                dec7_t1= numeric(length = nrow(yy)),
                dec8_t1= numeric(length = nrow(yy)),
                dec9_t1= numeric(length = nrow(yy)),
                dec0_t2= numeric(length = nrow(yy)),
                dec1_t2= numeric(length = nrow(yy)),
                dec2_t2= numeric(length = nrow(yy)),
                dec3_t2= numeric(length = nrow(yy)),
                dec4_t2= numeric(length = nrow(yy)),
                dec5_t2= numeric(length = nrow(yy)),
                dec6_t2= numeric(length = nrow(yy)),
                dec7_t2= numeric(length = nrow(yy)),
                dec8_t2= numeric(length = nrow(yy)),
                dec9_t2= numeric(length = nrow(yy)),
                dec0_t3= numeric(length = nrow(yy)),
                dec1_t3= numeric(length = nrow(yy)),
                dec2_t3= numeric(length = nrow(yy)),
                dec3_t3= numeric(length = nrow(yy)),
                dec4_t3= numeric(length = nrow(yy)),
                dec5_t3= numeric(length = nrow(yy)),
                dec6_t3= numeric(length = nrow(yy)),
                dec7_t3= numeric(length = nrow(yy)),
                dec8_t3= numeric(length = nrow(yy)),
                dec9_t3= numeric(length = nrow(yy)),
                dir0_t1= numeric(length = nrow(yy)),
                dir1_t1= numeric(length = nrow(yy)),
                dir2_t1= numeric(length = nrow(yy)),
                dir3_t1= numeric(length = nrow(yy)),
                dir4_t1= numeric(length = nrow(yy)),
                dir5_t1= numeric(length = nrow(yy)),
                dir6_t1= numeric(length = nrow(yy)),
                dir7_t1= numeric(length = nrow(yy)),
                dir8_t1= numeric(length = nrow(yy)),
                dir9_t1= numeric(length = nrow(yy)),
                dir0_t2= numeric(length = nrow(yy)),
                dir1_t2= numeric(length = nrow(yy)),
                dir2_t2= numeric(length = nrow(yy)),
                dir3_t2= numeric(length = nrow(yy)),
                dir4_t2= numeric(length = nrow(yy)),
                dir5_t2= numeric(length = nrow(yy)),
                dir6_t2= numeric(length = nrow(yy)),
                dir7_t2= numeric(length = nrow(yy)),
                dir8_t2= numeric(length = nrow(yy)),
                dir9_t2= numeric(length = nrow(yy)),
                dir0_t3= numeric(length = nrow(yy)),
                dir1_t3= numeric(length = nrow(yy)),
                dir2_t3= numeric(length = nrow(yy)),
                dir3_t3= numeric(length = nrow(yy)),
                dir4_t3= numeric(length = nrow(yy)),
                dir5_t3= numeric(length = nrow(yy)),
                dir6_t3= numeric(length = nrow(yy)),
                dir7_t3= numeric(length = nrow(yy)),
                dir8_t3= numeric(length = nrow(yy)),
                dir9_t3= numeric(length = nrow(yy)),
                prono= numeric(length = nrow(yy)),
                id = numeric(length = nrow(yy)),
                fecha = numeric(length = nrow(yy)) )



#library(profvis)

#profvis({
#library(foreach)
#library(doParallel)
#numCores <- detectCores()-1
#registerDoParallel(numCores) 

#xx <- xx[1:5000,]
#yy <- yy[1:5000,]
system.time(
  #foreach(point=1:(nrow(yy)),.combine=rbind,.packages=c('tidyverse','lubridate','geosphere')) %dopar% {
  for (point in 1:(nrow(yy))) {
    
    #Past
    #10min
    temp1<-xx %>% dplyr::filter(between(fecha,yy$Fecha[point]-minutes(t1),yy$Fecha[point]))
    
    if (nrow(temp1) != 0) {
      dist1<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp1$Long,temp1$Lat))
      
      feat1 <- quantile(dist1, prob = seq(0, 1, length = 11), type = 5)
      
      dir1 <- geosphere::bearing(p1 = cbind(yy$Long[point],yy$Lat[point]),
                                 p2 =cbind(temp1$Long,temp1$Lat))
      
      fd1 <- quantile(dir1, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat1 <- NA
      fd1 <- NA
    }
    
    
    rm(temp1)
    rm(dist1)
    rm(dir1)
    #20min
    temp2<-xx %>% dplyr::filter(between(fecha,yy$Fecha[point]-minutes(t2),yy$Fecha[point]))
    if (nrow(temp2) != 0) {
      dist2<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp2$Long,temp2$Lat))
      
      feat2 <- quantile(dist2, prob = seq(0, 1, length = 11), type = 5)
      
      dir2 <- geosphere::bearing(p1 = cbind(yy$Long[point],yy$Lat[point]),
                                 p2 =cbind(temp2$Long,temp2$Lat))
      
      fd2 <- quantile(dir2, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat2 <- NA
      fd2 <- NA
    }
    
    rm(temp2)
    rm(dist2)
    rm(dir2)
    
    #30min
    
    temp3<-xx %>% filter(between(fecha,yy$Fecha[point]-minutes(t3),yy$Fecha[point]))
    
    if (nrow(temp3) != 0) {
      dist3<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp3$Long,temp3$Lat))
      feat3 <- quantile(dist3, prob = seq(0, 1, length = 11), type = 5)
      
      dir3 <- geosphere::bearing(p1 = cbind(yy$Long[point],yy$Lat[point]),
                                 p2 =cbind(temp3$Long,temp3$Lat))
      
      fd3 <- quantile(dir3, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat3 <- NA
      fd3 <- NA
    }
    
    rm(temp3)
    rm(dist3)  
    rm(dir3)
    #Future
    temp_fut<-xx %>% filter(between(fecha,yy$Fecha[point],yy$Fecha[point]+minutes(10)))
    
    if (nrow(temp_fut)!=0) 
    {
      dist_fut<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                                  y = cbind(temp_fut$Long,temp_fut$Lat))
      
      if (min(dist_fut) < r1) {
        feat_fut <- 0 } 
      else if (between(min(dist_fut),r1,r2)) {
        feat_fut <- 1 } 
      else if (between(min(dist_fut),r2,r3)) {
        feat_fut <- 2 }
      else {feat_fut <- 3} 
    } 
    else {
      feat_fut <- NA
    }
    
    
    datos[point,1:10]<-feat1[1:10]
    datos[point,11:20]<-feat2[1:10]
    datos[point,21:30]<-feat3[1:10]
    datos[point,31:40]<-fd1[1:10]
    datos[point,41:50]<-fd2[1:10]
    datos[point,51:60]<-fd3[1:10]
    datos[point,61]<-feat_fut
    datos[point,62] <- yy$ID[point]
    datos[point,63] <- yy$Fecha[point]
    rm(temp_fut)
    rm(dist_fut)
    rm(feat1)
    rm(feat2)
    rm(feat3)
    rm(fd1)
    rm(fd2)
    rm(fd3)
  }
)


datos <- datos %>% drop_na()
#datos<-datos[sample(nrow(datos)),]
#datos<-datos[rowSums(!as.matrix(datos)) < ncol(datos), ]
name1 = toString(date())
name = paste(name1,'.csv',sep = '')
lugar <- paste('./',name,sep='')
write_csv(x = datos, path= lugar)
gc()



