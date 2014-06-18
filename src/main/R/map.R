library(rworldmap)

fa <- read.table("/export/scratch2/hannes/warcs/final.tsv",sep="\t",stringsAsFactors=F,header=F,quote="",col.names=c("country","class","count"))
fa$class <- factor(fa$class)
fs <- split(fa,fa$class)
fm <- merge(fs$clean,fs$porn,by="country")
fm$ratio = fm$count.y/fm$count.x

res <- data.frame(country=fm$country,clean=fm$count.x,dirty=fm$count.y,ratio=fm$ratio)
sPDF <- joinCountryData2Map(res,joinCode = "ISO2",nameJoinColumn = "country")
colfunc <- colorRampPalette(c("white", "#FF00FF"))
mapDevice(device="pdf",width=15,file="kinkymap.pdf")
mapCountryData(sPDF,nameColumnToPlot="ratio",mapTitle="",addLegend=F, numCats=10, colourPalette=colfunc(10))
dev.off()

print(res)