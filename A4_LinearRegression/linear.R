library(ggplot2)
setwd("~/workspace/Singh_Mehta_A4/output")

file_list <- list.files(pattern="*.txt")

dist_rsquared = list()
time_rsquared = list()
dist_mse = list()
time_mse = list()

dist_slope <- data.frame(Carrier = character(13), Slope = numeric(13), stringsAsFactors = FALSE)

time_slope <- data.frame(Carrier = character(13), Slope = numeric(13), stringsAsFactors = FALSE)

for(i in 1:13){
  
  dataset <- read.table(file_list[i], header=FALSE, sep="\t")
  names(dataset) <- c("Carrier", "Year", "Price", "Distance", "Time")
  
  carrier <- dataset$Carrier[i]
  plot1 <- "_Price-Distance"
  plot2 <- "_Price-Time"
  format= ".png"
  
  plotname = paste(carrier,plot1,format, sep="")
  png(filename=plotname)   
  
  lm1 <- lm(Price ~ Distance, data=dataset)
  p1 <- plot(Price ~ Distance, data = dataset, main=carrier)
  abline(lm1, col="red")
  
  sm <- summary(lm1)
  dist_rsquared[i] <- sm$adj.r.squared
  dist_mse[i] <-  mean(sm$residuals^2)
  
  dist_slope$Carrier[i] <- substring(file_list[i], 1 , 2)
  dist_slope$Slope[i] <- sm$coefficients[2][1]
  
  dev.off()
  
  plotname = paste(carrier,plot2,format, sep="")
  png(filename=plotname)  
  
  lm2 <- lm(Price ~ Time, data=dataset)
  p2 <- plot(Price ~ Time, data=dataset, main=carrier)
  abline(lm2, col="red")
  
  sm <- summary(lm2)
  time_rsquared[i] <- sm$adj.r.squared
  time_mse[i] <-  mean(sm$residuals^2)
  
  time_slope$Carrier[i] <- substring(file_list[i], 1 , 2)
  time_slope$Slope[i] <- sm$coefficients[2][1]
  
  dev.off()
}

# R-Squared Distance
avg_dist_rsq = mean(unlist(dist_rsquared))

# RMSE Distance
avg_dist_mse = mean(unlist(dist_mse))

#R-Squared Time
avg_time_rsq = mean(unlist(time_rsquared))

# RMSE Time
avg_time_mse = mean(unlist(time_mse))

if(avg_dist_rsq > avg_time_rsq) {
  print("Distance is a better variable")
  print(paste("Adjusted R-Squared for Price-Distance:", avg_dist_rsq))
  print(paste("Adjusted R-Squared for Price-Time:", avg_time_rsq))

  print("New Ranking of Airlines:")
  sorted <- dist_slope[order(dist_slope$Slope),] 
  
  sorted
  
} else{
  print("Time is a better variable")
  print(paste("Adjusted R-Squared for Price-Time:", avg_time_rsq))
  print(paste("Adjusted R-Squared for Price-Distance:", avg_dist_rsq))

  print("New Ranking of Airlines:")
  sorted <- time_slope[order(time_slope$Slope),] 
  
  sorted
}



  
