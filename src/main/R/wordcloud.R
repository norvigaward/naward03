library(wordcloud)
setwd("~/git/naward03/results/")
termweights <- read.table("words.tsv",sep="\t",header=F,quote="",col.names=c("term","weight"))
vocab <- read.table("../src/main/resources/vocab.tsv.gz",sep="\t",header=F,quote="",stringsAsFactors=F)
stopwords <- read.table("../src/main/resources/stopword-list.txt",sep="\t",header=F,quote="",stringsAsFactors=F)[[1]]

termweights <- vocab[vocab$V1=="porn",c(2,8)]
names(termweights) <- c("term","weight")
termweights <- termweights[!(termweights$term %in% stopwords),]

pdf("wordcloud.pdf",height=150,width=15)
wordcloud(termweights$term,termweights$weight,colors=brewer.pal(6,"Dark2"),max.words=150,min.freq=0)
text(.8,.2,"http://hannes.muehleisen.org",col="darkgray",pos=3,cex=.8)

dev.off()
# make a PNG for the Web
system("convert -opaque none -fill white -density 300 -trim  wordcloud.pdf wordcloud.png")