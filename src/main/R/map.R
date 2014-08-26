library(rworldmap)
library(knitr)

fa <- read.table("map.tsv",sep="\t",
	stringsAsFactors=F,header=F,quote="",col.names=c("country","dirty","count"))
fa$dirty <- as.logical(fa$dirty)
fa$dirty[is.na(fa$dirty)] <- FALSE

fs <- split(fa,fa$dirty)
fm <- merge(fs$`FALSE`,fs$`TRUE`,by="country")

fm$ratio = round((fm$count.y/(fm$count.x+fm$count.y))*100,2)
res <- data.frame(country=fm$country,clean=fm$count.x,dirty=fm$count.y,ratio=fm$ratio)

dsum <- sum(as.numeric(res$dirty))
csum <- sum(as.numeric(res$clean))
tsum <- dsum+csum

print(tsum)
print(csum)
print(dsum)
print(dsum/tsum)


res <- res[res$country != "" & res$clean > 10^6,]

sPDF <- joinCountryData2Map(res,joinCode = "ISO2",nameJoinColumn = "country")
colfunc <- colorRampPalette(c("#FFE5FF", "#FF00FF"))
cols <- colfunc(7)[1:6]

mapDevice(device="pdf",width=15,file="kinkymap.pdf")
legenddata <- mapCountryData(sPDF,nameColumnToPlot="ratio",mapTitle="",addLegend=F,numCats=6, colourPalette=cols,missingCountryCol="white")
#do.call(addMapLegend, c(legenddata,legendShrink=.3,labelFontSize=.8,horizontal=F,legendMar=5))
text(155,-65,"http://hannes.muehleisen.org",col="darkgray",pos=3,cex=.8)
dev.off()

geo <- read.csv("countryInfo.txt",sep="\t",stringsAsFactors=F)[c("ISO","Country")]
res <- merge(res,geo,by.x="country",by.y="ISO")
names(res) <- c("CC","Clean URLs","Dirty URLs","Ratio","Country")
res <- res[order(-res[4]),]

res$Ratio <- paste0(res$Ratio,"%")
res[2] <- format(res[2], big.mark = ",")
res[3] <- format(res[3], big.mark = ",")
table <- kable(res[c(5,2,3,4)], format = "markdown",align=c("l","r","r","r"),row.names=F)
cat(table,file="map.markdown",sep="\n")

# make a PNG for the Web
system("convert -opaque none -fill white -density 300 -crop -0-350  kinkymap.pdf kinkymap.png")
