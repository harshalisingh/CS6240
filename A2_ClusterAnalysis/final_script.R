library(ggplot2)

#Read the output file
table = read.table("finalOutput", sep="\t",col.names=c("Carrier", "Month", "Count", "Avg_Price"))

#Sort the data and fetch top 10 flights based on count
sorted_df <- table[order(-table$Count),]
top10 <- sorted_df[1:10,]

top10$Month <- month.abb[top10$Month]

#Plot the average ticket price for each month for each airline
p <- ggplot(top10, aes(Carrier, Avg_Price)) + geom_point()
p + facet_grid(. ~ Month)
ggsave(filename="Topten.png")
