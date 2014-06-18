library(rworldmap)

fa <- read.table("/export/scratch2/hannes/warcs/final.tsv",sep="\t",
	stringsAsFactors=F,header=F,quote="",col.names=c("country","dirty","count"))
fa$dirty <- as.logical(fa$dirty)
fs <- split(fa,fa$dirty)
fm <- merge(fs$`FALSE`,fs$`TRUE`,by="country")
fm$ratio = fm$count.y/fm$count.x

res <- data.frame(country=fm$country,clean=fm$count.x,dirty=fm$count.y,ratio=fm$ratio)
res <- res[res$country != "" & res$clean > 100,]

sPDF <- joinCountryData2Map(res,joinCode = "ISO2",nameJoinColumn = "country")
colfunc <- colorRampPalette(c("white", "#FF00FF"))
mapDevice(device="pdf",width=15,file="kinkymap.pdf")
mapCountryData(sPDF,nameColumnToPlot="ratio",mapTitle="",addLegend=F, numCats=10, colourPalette=colfunc(10))

dev.off()

geo <- read.csv("/export/scratch2/hannes/geonames/countryInfo.txt",sep="\t",stringsAsFactors=F)[c("ISO","Country")]
res <- merge(res,geo,by.x="country",by.y="ISO")
print(res)
