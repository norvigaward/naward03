library(wordcloud)
library(knitr)

setwd("~/git/naward03/results/")
vocab <- read.table("../src/main/resources/vocab.tsv.gz",sep="\t",header=F,quote="",stringsAsFactors=F)
stopwords <- read.table("../src/main/resources/stopword-list.txt",sep="\t",header=F,quote="",stringsAsFactors=F)[[1]]

termweights <- vocab[vocab$V2=="porn",c(1,3)]
names(termweights) <- c("term","weight")
termweights <- termweights[!(termweights$term %in% stopwords),]
termweights <- termweights[order(-termweights$weight),]

pdf("wordcloud.pdf",height=15,width=15)
wordcloud(termweights$term,termweights$weight,colors=brewer.pal(6,"Dark2"),max.words=150,min.freq=0)
text(.8,.2,"http://hannes.muehleisen.org",col="darkgray",pos=3,cex=.8)

dev.off()
# make a PNG for the Web
system("convert -opaque none -fill white -density 300 -trim  wordcloud.pdf wordcloud.png")

names(termweights) <- c("Word","In-Class Frequency")
table <- kable(termweights[1:20,], format = "markdown",align=c("l","r"),row.names=F)
cat(table,file="wordcloud.markdown",sep="\n")