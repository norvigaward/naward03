library(wordcloud)
termweights <- read.table("words.tsv",sep="\t",header=F,quote="",col.names=c("term","weight"))
pdf("wordcloud.pdf",height=15,width=15)
wordcloud(termweights$term,termweights$weight,colors=brewer.pal(6,"Dark2"))
dev.off()
# make a PNG for the Web
system("convert -opaque none -fill white -density 300 -trim  wordcloud.pdf wordcloud.png")