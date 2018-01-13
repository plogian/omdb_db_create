#change the working directory
#setwd("C:/Users/Lillian/Documents/R")
library(jsonlite)
library(dplyr)
library(RMySQL)

omdb_api_key <- "omdb api key here"
database_name <- "database name here"
host_name<- "host name here"
db_username <- "database username here"
db_password <- "database password here"



#record the time when the scrape starts 
writeLines(text=as.character(Sys.time()), file("startTime.txt"))
startTimeFileConn <- file("startTime.txt")
startTime <- readLines(startTimeFileConn)
close(startTimeFileConn)

startTime <- as.POSIXct(startTime)
#endTime is 15 hours after startTime 60*60*15
endTime <- startTime + 60*60*15

#initiate empty dataframe
omdb_list <- data.frame(Title= character(), Year= integer(), Rated = character(), Released=character(), 
                        Runtime = character(), Genre = character(), Director = character(), Writer= character(),
                        Actors = character(), Plot=character(), Language=character(), Country=character(), Awards=character(), 
                        Poster= character(), imdbRating = character(), RottenTomatoes=character(), Metascore=character(),
                        imdbRating2 = character(),  imdbVotes=character(), imdbID=character(), DVD=character(), 
                        BoxOffice= character(), Production=character(), Website=character(), Response= character(), 
                        Season=character(), Episode=character(), totalSeasons=character(), metacritic=character(), Type=character(), stringsAsFactors = FALSE)

rownames <- c("Title", "Year", "Rated", "Released", "Runtime", "Genre", "Director", "Writer", "Actors",
                         "Plot", "Language", "Country", "Awards", "Poster", "imdbRating", "RottenTomatoes", "Metascore",
                         "imdbRating2", "imdbVotes", "imdbID", "DVD", "BoxOffice", "Production", "Website", "Response", 
                         "Season", "Episode", "totalSeasons", "Metacritic", "Type")

#remove letters with accent marks
unwanted_array = list(    'S'='S', 's'='s', 'Z'='Z', 'z'='z', 'À'='A', 'Á'='A', 'Â'='A', 'Ã'='A', 'Ä'='A', 'Å'='A', 'Æ'='A', 'Ç'='C', 'È'='E', 'É'='E',
                          'Ê'='E', 'Ë'='E', 'Ì'='I', 'Í'='I', 'Î'='I', 'Ï'='I', 'Ñ'='N', 'Ò'='O', 'Ó'='O', 'Ô'='O', 'Õ'='O', 'Ö'='O', 'Ø'='O', 'Ù'='U',
                          'Ú'='U', 'Û'='U', 'Ü'='U', 'Ý'='Y', 'Þ'='B', 'ß'='Ss', 'à'='a', 'á'='a', 'â'='a', 'ã'='a', 'ä'='a', 'å'='a', 'æ'='a', 'ç'='c',
                          'è'='e', 'é'='e', 'ê'='e', 'ë'='e', 'ì'='i', 'í'='i', 'î'='i', 'ï'='i', 'ð'='o', 'ñ'='n', 'ò'='o', 'ó'='o', 'ô'='o', 'õ'='o',
                          'ö'='o', 'ø'='o', 'ù'='u', 'ú'='u', 'û'='u', 'ý'='y', 'ý'='y', 'þ'='b', 'ÿ'='y' )

translit <- function(x) {
  chartr(paste(names(unwanted_array), collapse=''),
         paste(unwanted_array, collapse=''),
         x)
}

failed_list <- c()

update_db <- function(start, end) {
  a <- c(start:end)
  b<- sprintf("%07d", a)
  
  for(i in 1:length(b)) {
    url <- paste0("http://omdbapi.com/?i=tt", b[i], "&plot=Full&r=json&apikey=", omdb_api_key)
    print(url)
    
    row <-  start-i+ 1
    
    tryCatch({ movie_data <- fromJSON(url)}, 
             error=function(e) {failed_list <- c(failed_list, b[i])})
    for(j in c(1:15, 17, 19:28, 30)) {
      if(length(movie_data[rownames[j]][[1]])>0 & isTRUE(movie_data[rownames[j]][[1]] != "N/A")) {
        omdb_list[row, j] <- as.character(movie_data[rownames[j]][[1]])
      } else {
        omdb_list[row, j] <- "NA"
      }
    }
    
    for(j in c(16)) {
      if(length(which(movie_data$Ratings$Source=="Rotten Tomatoes"))>0) {
        omdb_list[row, j] <- as.numeric(sub("%", "", movie_data$Ratings[which(movie_data$Ratings$Source=="Rotten Tomatoes"), 2]))
      } else {
        omdb_list[row, j] <- "NA"
      }
    }
    for(j in c(29)) {
      if(length(which(movie_data$Ratings$Source=="Metacritic"))>0) {
        omdb_list[row, j] <- as.numeric(sub("/100", "", movie_data$Ratings[which(movie_data$Ratings$Source=="Metacritic"), 2]))
      } else {
        omdb_list[row, j] <- "NA"
      }
    }
    
    for(j in c(18)) {
      if(length(which(movie_data$Ratings$Source=="Internet Movie Database"))>0) {
        omdb_list[row, j] <- as.numeric(sub("/10", "", movie_data$Ratings[which(movie_data$Ratings$Source=="Internet Movie Database"), 2]))
      } else {
        omdb_list[row, j] <- "NA"
      } 
    }
    #break scrape into 100,000 unit chunks to speed up the process
    if(i%%1000000==0) {
      write.csv(omdb_list, paste0("omdb_list_", b[i], ".csv"))
      assign(paste0("omdb_list", b[i]), omdb_list)
      omdb_list <- data.frame(Title= character(), Year= integer(), Rated = character(), Released=character(), 
                              Runtime = character(), Genre = character(), Director = character(), Writer= character(),
                              Actors = character(), Plot=character(), Language=character(), Country=character(), Awards=character(), 
                              Poster= character(), imdbRating = character(), RottenTomatoes=character(), Metascore=character(),
                              imdbRating2 = character(),  imdbVotes=character(), imdbID=character(), DVD=character(),
                              BoxOffice= character(), Production=character(), Website=character(), Response= character(), 
                              Season=character(), Episode=character(), totalSeasons=character(), metacritic=character(), Type=character(), stringsAsFactors = FALSE)
    }
  }
  
  omdb_master <- omdb_list
  
  omdb_master$Title <- translit(omdb_master$Title)
  omdb_master$Rated <- translit(omdb_master$Rated)
  omdb_master$Director <- translit(omdb_master$Director)
  omdb_master$Writer <- translit(omdb_master$Writer)
  omdb_master$Actors <- translit(omdb_master$Actors)
  omdb_master$Plot <- translit(omdb_master$Plot)
  omdb_master$Language <- translit(omdb_master$Language)
  omdb_master$Country <- translit(omdb_master$Country)
  omdb_master$Awards <- translit(omdb_master$Awards)
  omdb_master$Poster <- translit(omdb_master$Poster)
  omdb_master$Production <- translit(omdb_master$Production)
  omdb_master$Website <- translit(omdb_master$Website)
  
  omdb_master$Released <- as.Date(omdb_master$Released, "%d %b %Y")
  omdb_master$DVD <- as.Date(omdb_master$DVD , "%d %b %Y")
  omdb_master$imdbRating <- as.numeric(omdb_master$imdbRating)
  omdb_master$Metascore <- as.numeric(omdb_master$Metascore)
  omdb_master$Season <- as.numeric(omdb_master$Season)
  omdb_master$Episode <- as.numeric(omdb_master$Episode)
  omdb_master$totalSeasons <- as.numeric(omdb_master$totalSeasons)
  omdb_master$RottenTomatoes <- as.numeric(omdb_master$RottenTomatoes)
  omdb_master$imdbVotes <- as.numeric(gsub(",", "", omdb_master$imdbVotes))
  omdb_master$BoxOffice <- as.numeric(gsub("\\$", "", gsub(",", "", omdb_master$BoxOffice)))
  
  omdb_master <- omdb_master[, -c(2, 18, 25, 29)]
  if(length(which(is.na(omdb_master$imdbID)))>0) {
    omdb_master <- omdb_master[-c(which(is.na(omdb_master$imdbID))),]
  }
  if(length(which(is.na(omdb_master$Title)))>0) {
    omdb_master <- omdb_master[-c(which(is.na(omdb_master$Title))),]
  }
  
  columns <- paste(names(omdb_master), collapse= ",")
  values <-   paste(sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %f, %g, %g, %g,'%s', '%s', %g, '%s', '%s', %g, %g, %g, '%s')",
                            gsub("\'", "\'\'", omdb_master$Title), omdb_master$Rated, gsub("-", "/", omdb_master$Released), omdb_master$Runtime, omdb_master$Genre,
                            gsub("\'", "\'\'", omdb_master$Director), gsub("\'", "\'\'", omdb_master$Writer), gsub("\'", "\'\'", omdb_master$Actors), gsub("\'", "\'\'", omdb_master$Plot), gsub("\'", "\'\'",omdb_master$Language),
                            gsub("\'", "\'\'",omdb_master$Country), gsub("\'", "\'\'", omdb_master$Awards), gsub("\'", "\'\'",omdb_master$Poster), omdb_master$imdbRating, omdb_master$RottenTomatoes,
                            omdb_master$Metascore, omdb_master$imdbVotes, omdb_master$imdbID, gsub("-", "/", omdb_master$DVD), omdb_master$BoxOffice,
                            gsub("\'", "\'\'",omdb_master$Production), gsub("\'", "\'\'",omdb_master$Website), omdb_master$Season, omdb_master$Episode, omdb_master$totalSeasons,
                            omdb_master$Type), collapse=', ')
  
  values <- gsub("NA,", "NULL,", values)
  strSQL <- paste0(
    'insert into omdbunique (', columns, ') values',
    values, ' on duplicate key update `Title`= VALUES(`Title`), `Rated`=VALUES(`Rated`), `Released`=VALUES(`Released`), `Runtime`= VALUES(`Runtime`), `Genre`= VALUES(`Genre`), `Director`=VALUES(`Director`), `Writer`=VALUES(`Writer`), `Actors` = VALUES(`Actors`), `Plot`=VALUES(`Plot`), `Language`=VALUES(`Language`), `Country`=VALUES(`Country`), `Awards`=VALUES(`Awards`), `Poster`=VALUES(`Poster`), `imdbRating`=VALUES(`imdbRating`), `RottenTomatoes`= VALUES(`RottenTomatoes`), `Metascore`=VALUES(`Metascore`), `imdbVotes`=VALUES(`imdbVotes`),`DVD`=VALUES(`DVD`), `BoxOffice`=VALUES(`BoxOffice`), `Production`=VALUES(`Production`), `Website`=VALUES(`Website`), `Season`=VALUES(`SEASON`), `Episode`= VALUES(`Episode`), `totalSeasons`= VALUES(`totalSeasons`), `Type` = VALUES(`Type`)'
  )
  
  dbName <- database_name 
  hostName <- host_name
  username <- db_username
  password <- db_password

  
  tryCatch({
    mydb <- dbConnect(MySQL(), user=username, password=password, dbname= dbName, host=hostName)
    dbGetQuery(mydb, strSQL)
    # dbWriteTable(mydb, "omdbunique2", omdb_master, append=TRUE)
    lapply(dbListConnections(MySQL()), dbDisconnect)
  }, error=function(e) {
    
  })

}

#the imdbID that the update is on is saved in counter.txt that determines where update_db begins its update
if(!file.exists("counter.txt")) {
  writeLines("1", file("counter.txt"))
}

while(Sys.time() < endTime) {
  fileConn <- file("counter.txt")
  counter <- readLines(fileConn)
  close(fileConn)
  
  counter <- as.numeric(counter)
  #add line increase top limit by .5 mil every year
  top_limit <- 7500000
  
  if(counter < top_limit) {
    startCount <- counter + 1
  } else {
    startCount <- 1
  }
  endCount <- counter + 1001
  
  
  update_db(startCount, endCount)
  
  fileConn <- file("counter.txt")
  counter <- writeLines(as.character(endCount), fileConn)
  close(fileConn)
}
