#setwd("C:/Users/daoban/OneDrive/Rayos/")

library(getPass)
library(RODBC) 
library(tidyverse)
library(lubridate)
library(keras)


#yy <- tibble(Fecha = ymd_hms(now()),Lat = 11.3780, Long = -82.8309)



modelo <- load_model_hdf5('./Automatizacion/lstm.h5)

#parametros
tmax <- 1800
nmin <- 10
dmax <- .05

r1<-2000
r2<-5000
r3<-10000
t1<-10
t2<-20
t3<-30

BDHidro <- odbcConnect("sihid", uid="daoban", pwd = getPass(), believeNRows=FALSE)

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

# yy = Crear Tabla de latitud, longitud y fecha de los puntos activos.
yy <- usuarios(BDHidro)


# consulta =funcion trae descargas fecha, latitud y longitud.
# as_tibble data frame en R para hacer consulta rapida en tablas.
# mutate, crea 3 columna que le interesan.
# select - quita columnas que no le interesan.
# arrange por fecha del viejo al nuevo.
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


data <- read.csv('Flash17_18.txt') %>% 
  as_tibble() %>% 
  mutate(fecha = paste(Anno,Mes,Dia,Hora,Min) %>% ymd_hm()) %>% 
  select(fecha, Lat, Long, KiloAmp) %>% 
  arrange(fecha) 

complete <- data

data <- complete[50000:400000,]

yy<- tibble(fecha=paste(2017,04,25,04,00)%>% ymd_hm(),Lat=9.942084,Long=-84.032504)

gc()


# liberar memoria.


# identifica clusters de tormenta.
clusters <- function(data,tmax,nmin,dmax){
  
  data %>% 
    mutate(fecha2 = as.numeric(fecha),
           deltat = fecha2 - lag(fecha2),
           deltat = replace_na(deltat,0),
           ind1 = (deltat > tmax),
           cluster1 = cumsum(ind1))  %>% 
    group_by(cluster1) %>% 
    mutate(n=n()) %>% 
    ungroup() %>% 
    filter(n>nmin) %>% 
    group_split(cluster1) %>%
    map(.f = function(df){
      
      res <- cbind(df$Long, df$Lat) %>% 
        dbscan::frNN(eps = dmax) %>% 
        dbscan::dbscan(minPts = nmin)
      
      df$cluster2 <- res$cluster
      rm(res); gc()
      
      df %>% filter(cluster2>0)
    }) %>% 
    bind_rows() %>% 
    group_by(cluster1,cluster2) %>% 
    mutate(fecha3 = min(fecha2)) %>% 
    ungroup() %>% 
    mutate(cluster3 = paste(fecha3, cluster2), 
           cluster4 = factor(cluster3),
           cluster = as.numeric(cluster4)) %>% 
    arrange(cluster) %>% 
    group_by(cluster) %>% 
    mutate(n2 = n()) %>% 
    ungroup() %>% 
    select(-fecha2:-ind1, -n:-cluster4) %>% 
    mutate(cluster1 = as.factor(cluster1),
           cluster_temporal = as.numeric(cluster1)) %>% 
    select(-cluster1)

}


#calcula la matriz de distancia de las tormentas con las reglas
mdist <- function(xx,yy,r1,r2,r3,t1,t2,t3){
  
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
                  prono= numeric(length = nrow(yy))  )
  
  
  for (point in 1:(nrow(yy))) {
    
    #Past
    #10min
    temp1<-xx %>% filter(between(fecha,yy$Fecha[point]-minutes(t1),yy$Fecha[point]))
    
    if (nrow(temp1) != 0) {
      dist1<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp1$Long,temp1$Lat))
      
      feat1 <- quantile(dist1, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat1 <- NA
    }
    
    
    rm(temp1)
    rm(dist1)
    
    #20min
    temp2<-xx %>% filter(between(fecha,yy$Fecha[point]-minutes(t2),yy$Fecha[point]))
    if (nrow(temp2) != 0) {
      dist2<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp2$Long,temp2$Lat))
      feat2 <- quantile(dist2, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat2 <- NA
    }
    
    rm(temp2)
    rm(dist2)
    
    #30min
    
    temp3<-xx %>% filter(between(fecha,yy$Fecha[point]-minutes(t3),yy$Fecha[point]))
    
    if (nrow(temp3) != 0) {
      dist3<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                               y = cbind(temp3$Long,temp3$Lat))
      feat3 <- quantile(dist3, prob = seq(0, 1, length = 11), type = 5)
      
    } else {
      
      feat3 <- NA
    }
    
    rm(temp3)
    rm(dist3)  
    
    #Future
    temp_fut<-xx %>% filter(between(fecha,yy$Fecha[point],yy$Fecha[point]+minutes(10)))
    
    if (nrow(temp_fut)!=0) {
      dist_fut<- geosphere::distm(x = cbind(yy$Long[point],yy$Lat[point]),
                                  y = cbind(temp_fut$Long,temp_fut$Lat))
      
      if (min(dist_fut) < r1) {
        feat_fut <- 0
      } else if (between(min(dist_fut),r1,r2)) {
        feat_fut <- 1
      } else {
        feat_fut <- 2
      }
      
    } else {
      feat_fut <- NA
    }
    
    
    datos[point,1:10]<-feat1[1:10]
    datos[point,11:20]<-feat2[1:10]
    datos[point,21:30]<-feat3[1:10]
    datos[point,31]<-feat_fut
    rm(temp_fut)
    rm(dist_fut)
    rm(feat1)
    rm(feat2)
    rm(feat3)
  }
  
  datos
}


#mismo tamaño
# equ , todos los radios tengan la misma cantidad de datos
equ <- function(datos){
  
  datos <- datos %>% drop_na()
  new<- datos %>% filter(prono==2) %>% sample_n(nrow(datos %>% filter(prono==1)))
  datos<- datos[!datos$prono ==2,] %>% bind_rows(new)
  datos<-datos[sample(nrow(datos)),]
  datos<-datos[rowSums(!as.matrix(datos)) < ncol(datos), ]
  datos
}

# procesado llama las funciones anteriores, procesa y llama al modelo
procesado <- function(BDHidro,modelo,yy,tmax=1800,nmin=10,dmax=0.05,r1=2000,r2=5000,r3=10000,t1=10,t2=20,t3=30){

  data <- consulta(BDHidro)
  xx <- clusters(data,tmax,nmin,dmax)
  if (nrow(xx)==0) {
    print("No hay clusters")
  } else {
    datos <- mdist(xx,yy,r1,r2,r3,t1,t2,t3)
    
    f <- equ(datos)
    
    mean <- c(16100.12922972,  33955.36664221,  46515.26069274,  58661.43087718,
              70910.3766115 ,  83956.65142416,  98052.98782099, 114292.12747534,
              134004.3412069 , 161258.31323625,  14300.51719032,  34439.82398748,
              47332.8757069 ,  59757.84200305,  72260.9456012 ,  85464.7194767 ,
              99863.23809849, 116287.27662039, 136465.71730273, 164063.99570075,
              13296.46489262,  35104.95083195,  48342.60715235,  61119.84824878,
              73841.3102905 ,  87184.12247895, 101763.31721207, 118288.85505716,
              138772.49534582, 166717.90406178)
    
    sd <- c(29859.133261  , 44204.96205881, 53546.38581097, 61567.94450569,
            +        68450.17896902, 74856.87947894, 80615.29514308, 86391.55129746,
            +        92192.83137311, 98171.42130425, 27711.21046103, 44135.10295211,
            +        53667.51715985, 61730.16704009, 68626.2771802 , 74937.73091608,
            +        80660.66274827, 86305.89452303, 92162.17620563, 97796.00989539,
            +        26431.92183979, 44300.07893112, 53799.77221524, 61941.07866762,
            +        68851.05861745, 75012.90407609, 80679.80130847, 86106.31075534,
            +        91958.86131722, 97533.21735509)
    
    
    obs <- (f[1:30]-mean)/sd
    
    
    resultado <- modelo %>% predict_classes(as.matrix(obs))
    
    resul_finales <- tibble(ID = yy$ID, Prono = resultado)
    
  }
  
}




# llama a todo y ejecuta.
pronostico <- procesado(BDHidro,modelo,yy)


#write_csv()

#dormir por 3 minutos
sleepFunction(180)
