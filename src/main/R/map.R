library(rworldmap)
library(knitr)

fa <- read.table("map.tsv",sep="\t",
	stringsAsFactors=F,header=F,quote="",col.names=c("country","dirty","count"))
fa$dirty <- as.logical(fa$dirty)
fa$dirty[is.na(fa$dirty)] <- FALSE

fs <- split(fa,fa$dirty)
fm <- merge(fs$`FALSE`,fs$`TRUE`,by="country")
fm$ratio = fm$count.y/fm$count.x

res <- data.frame(country=fm$country,clean=fm$count.x,dirty=fm$count.y,ratio=fm$ratio)
res <- res[res$country != "" & res$dirty > 10,]

sPDF <- joinCountryData2Map(res,joinCode = "ISO2",nameJoinColumn = "country")
colfunc <- colorRampPalette(c("white", "#FF00FF"))

mapDevice(device="pdf",width=15,file="kinkymap.pdf")
legenddata <- mapCountryData(sPDF,nameColumnToPlot="ratio",mapTitle="",addLegend=F,numCats=10, colourPalette=colfunc(10),missingCountryCol="lightgray")
do.call(addMapLegend, c(legenddata,legendShrink=.3,labelFontSize=.8,horizontal=F,digits=2,legendMar=5))

text(155,-65,"http://hannes.muehleisen.org",col="darkgray",pos=3,cex=.8)
dev.off()

geo <- read.csv("countryInfo.txt",sep="\t",stringsAsFactors=F)[c("ISO","Country")]
res <- merge(res,geo,by.x="country",by.y="ISO")
names(res) <- c("CC","Clean URLs","Dirty URLs","Ratio","Country")
res <- res[order(-res[4]),]
print(res)

res$Ratio <- paste0(round(res$Ratio*100),"%")
res[2] <- format(res[2], big.mark = ",")
res[3] <- format(res[3], big.mark = ",")
table <- kable(res[c(5,2,3,4)], format = "markdown",align=c("l","r","r","r"))
cat(table,file="ratio.markdown",sep="\n")

# make a PNG for the Web
system("convert -opaque none -fill white -density 300 -crop -0-350  kinkymap.pdf kinkymap.png")
