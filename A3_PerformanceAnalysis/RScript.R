library(ggplot2)

#Read the output file
table = read.csv("benchmarking.csv", sep=",");

#Plot the average ticket price for each month for each airline
p <- ggplot(table, aes(CONF, TIME)) + geom_point();
p + facet_grid(. ~ MODE);

#save the file as pdf
ggsave(filename= "Rplot.pdf");
