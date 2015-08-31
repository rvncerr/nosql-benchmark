trim <- function (x) gsub("^\\s+|\\s+$", "", x)

nb.ycsb.load <- function(path) {
  # Loading YCSB data.
  csv <- read.csv(path, header=FALSE)
  csv
}

nb.ycsb.histogram <- function(csv, keyword) {
  # Creating histogram.
  valid <- 0:999
  valid <- as.character(valid)
  valid <- append(valid, ">1000")
  csv2 <- csv[csv$V1 == keyword, c("V2", "V3")]
  csv2 <- csv2[trim(as.character(csv2$V2)) %in% valid, c("V2", "V3")]
  m <- t(data.matrix(csv2$V3))
  colnames(m) <- csv2$V2
  m
}

csv <- nb.ycsb.load("...")
cleanup_h <- nb.ycsb.histogram(csv, "[CLEANUP]")
barplot(cleanup_h)
read_h <- nb.ycsb.histogram(csv, "[READ]")
barplot(read_h)